package fcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cacheEntry struct {
	keyHash  KeyHash
	sequence uint64
	size     int64
	mtime    int64
	expires  int64
	prev     *cacheEntry
	next     *cacheEntry
}

func (e *cacheEntry) isExpired() bool {
	return e.expires > 0 && time.Now().UnixMilli() > e.expires
}

func (e *cacheEntry) isValid() bool {
	return !e.isExpired()
}

func (e *cacheEntry) Info() *EntryInfo {
	info := &EntryInfo{Size: e.size, Mtime: time.UnixMilli(e.mtime)}
	if e.expires > 0 {
		info.Expires = time.UnixMilli(e.expires)
	}
	return info
}

type cache struct {
	cacheDir   string
	targetSize int64
	dirMode    os.FileMode
	fileMode   os.FileMode

	usedSize  int64
	has       atomic.Int64
	gets      atomic.Int64
	hits      atomic.Int64
	puts      atomic.Int64
	deletes   atomic.Int64
	evictions int64

	sequence atomic.Uint64
	lock     sync.RWMutex
	entries  map[KeyHash]*cacheEntry
	first    *cacheEntry
	last     *cacheEntry
	locker   *Locker

	evictionLock     sync.RWMutex
	evictionInterval time.Duration
	evictionTime     time.Time
	evictionDuration time.Duration
	evictionErrors   []EvictionError

	clearOrEvictDoingDeletes atomic.Int32
}

var _ Cache = (*cache)(nil)

func (c *cache) Stats() Stats {
	stats := Stats{
		Has:     c.has.Load(),
		Gets:    c.gets.Load(),
		Hits:    c.hits.Load(),
		Puts:    c.puts.Load(),
		Deletes: c.deletes.Load(),
		Locks:   c.locker.Size(),
	}
	c.lock.RLock()
	stats.Items = len(c.entries)
	stats.Bytes = c.usedSize
	c.lock.RUnlock()

	c.evictionLock.RLock()
	stats.Evictions = c.evictions
	stats.EvictionTime = c.evictionTime
	stats.EvictionDuration = c.evictionDuration
	stats.EvictionErrors = c.evictionErrors
	c.evictionLock.RUnlock()

	return stats
}

func (c *cache) Has(key Key) (*EntryInfo, error) {
	keyHash := key.ToHash()
	c.has.Add(1)

	c.lock.RLock()
	entry, ok := c.entries[keyHash]
	c.lock.RUnlock()

	if ok {
		c.locker.RLock(keyHash)
		defer c.locker.RUnlock(keyHash)

		if entry.isValid() {
			return entry.Info(), nil
		}
	}

	return nil, ErrNotFound
}

func (c *cache) Put(key Key, val []byte, ttl time.Duration) (*EntryInfo, error) {
	return c.PutReader(key, bytes.NewReader(val), ttl)
}

func (c *cache) PutReader(key Key, r io.Reader, ttl time.Duration) (*EntryInfo, error) {
	keyHash := key.ToHash()
	c.locker.Lock(keyHash)
	defer c.locker.Unlock(keyHash)

	entry, _, err := c.putEntry(key, ttl, false, FillerFunc(func(key Key, sink io.Writer) (written int64, err error) {
		return io.Copy(sink, r)
	}))
	if err != nil {
		return nil, err
	}

	return entry.Info(), nil
}

func (c *cache) Get(key Key) ([]byte, *EntryInfo, error) {
	r, info, err := c.GetReader(key)
	if err != nil {
		return nil, info, err
	}
	defer func() {
		_ = r.Close()
	}()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, info, err
	}
	return data, info, nil
}

func (c *cache) GetReader(key Key) (io.ReadSeekCloser, *EntryInfo, error) {
	keyHash := key.ToHash()
	entry := c.getEntry(keyHash)
	if entry == nil {
		return nil, nil, ErrNotFound
	}

	c.locker.RLock(keyHash)
	defer c.locker.RUnlock(keyHash)

	if !entry.isValid() {
		return nil, nil, ErrNotFound
	}

	c.hits.Add(1)

	f, err := os.Open(c.buildEntryPath(entry.keyHash, entry.mtime, entry.expires, entry.sequence))
	if err != nil {
		// pretend the entry does not exist, its file was likely just removed by a clear or evict
		if c.clearOrEvictDoingDeletes.Load() > 0 && errors.Is(err, os.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	return f, entry.Info(), nil
}

func (c *cache) GetOrPut(key Key, ttl time.Duration, filler Filler) (data []byte, info *EntryInfo, hit bool, err error) {
	r, info, hit, err := c.GetReaderOrPut(key, ttl, filler)
	if err != nil {
		return nil, info, hit, err
	}
	defer func() {
		err = r.Close()
	}()

	data, err = io.ReadAll(r)
	if err != nil {
		return nil, info, hit, err
	}
	return data, info, hit, nil
}

func (c *cache) GetReaderOrPut(key Key, ttl time.Duration, filler Filler) (r io.ReadSeekCloser, info *EntryInfo, hit bool, err error) {
retry:
	keyHash := key.ToHash()
	c.locker.RLock(keyHash)
	entry := c.getEntry(keyHash)
	if entry != nil && entry.isValid() {
		c.hits.Add(1)

		r, err = os.Open(c.buildEntryPath(entry.keyHash, entry.mtime, entry.expires, entry.sequence))
		if err != nil {
			// the file was likely just removed by a clear or evict, so we ignore the err
			if c.clearOrEvictDoingDeletes.Load() > 0 && errors.Is(err, os.ErrNotExist) {
				c.hits.Add(-1) // don't count this as hit
				goto put
			}
			c.locker.RUnlock(keyHash)
			return nil, nil, false, err
		}
		info = entry.Info()
		c.locker.RUnlock(keyHash)
		return r, info, true, nil
	}
put:
	// entry does not exist or is expired, so we need to do a put.
	// Upgrade the read lock to a write lock, if this fails another goroutine is already doing a put.
	if !c.locker.Upgrade(keyHash) {
		c.locker.RUnlock(keyHash)
		goto retry
	}
	defer c.locker.Unlock(keyHash)

	entry, r, err = c.putEntry(key, ttl, true, filler)
	if err != nil {
		return nil, nil, false, err
	}

	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		_ = r.Close()
		return nil, nil, false, err
	}

	return r, entry.Info(), false, nil
}

func (c *cache) Delete(key Key) (*EntryInfo, error) {
	keyHash := key.ToHash()

	c.locker.Lock(keyHash)
	defer c.locker.Unlock(keyHash)

	c.deletes.Add(1)

	c.lock.RLock()
	entry, ok := c.entries[keyHash]
	c.lock.RUnlock()
	if ok {
		err := os.Remove(c.buildEntryPath(entry.keyHash, entry.mtime, entry.expires, entry.sequence))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		c.lock.Lock()
		c.removeEntry(entry)
		c.lock.Unlock()

		return entry.Info(), nil
	}

	return nil, nil
}

func (c *cache) Clear(resetStats bool) error {
	c.lock.Lock()
	deletedEntries := c.entries
	c.deletes.Add(int64(len(c.entries)))
	c.entries = make(map[KeyHash]*cacheEntry)
	c.usedSize = 0
	c.lock.Unlock()

	if resetStats {
		c.has.Store(0)
		c.gets.Store(0)
		c.hits.Store(0)
		c.puts.Store(0)
		c.deletes.Store(0)
		c.evictionLock.Lock()
		c.evictions = 0
		c.evictionLock.Unlock()
	}

	// delete actual files in "background" so cache can be used again before Clear returns
	c.clearOrEvictDoingDeletes.Add(1)
	defer c.clearOrEvictDoingDeletes.Add(-1)

	delErrors := 0
	var delErr error
	for _, entry := range deletedEntries {
		err := os.Remove(c.buildEntryPath(entry.keyHash, entry.mtime, entry.expires, entry.sequence))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			delErrors++
			delErr = err
		}
	}
	if delErr != nil {
		if delErrors > 1 {
			return fmt.Errorf("encountered %d errors during clearing. The last one was: %w", delErrors, delErr)
		} else {
			return delErr
		}
	}

	return nil
}

// createShardDirs creates all possible shard dirs
func (c *cache) createShardDirs() error {
	for i := 0; i < maxShards; i++ {
		shardDir := filepath.Join(c.cacheDir, shard(i).String())
		err := os.MkdirAll(shardDir, c.dirMode)
		if err != nil {
			return fmt.Errorf("failed to create shard dir %s: %w", shardDir, err)
		}
	}
	return nil
}

func (c *cache) buildEntryPath(keyHash KeyHash, mtime int64, expires int64, sequence uint64) string {
	shard, name := toFilename(keyHash, mtime, expires, sequence)
	return filepath.Join(c.cacheDir, shard, name)
}

func (c *cache) moveToFront(entry *cacheEntry) {
	if c.first != entry {
		entry.prev.next = entry.next
		if entry.next != nil {
			entry.next.prev = entry.prev
		} else {
			c.last = entry.prev
		}

		entry.next = c.first
		entry.prev = nil
		if c.first != nil {
			c.first.prev = entry
		}
		c.first = entry
	}
}

func (c *cache) removeEntry(entry *cacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	}
	if c.first == entry {
		c.first = entry.next
	}
	if c.last == entry {
		c.last = entry.prev
	}

	entry.next = nil // avoid memory leaks
	entry.prev = nil // avoid memory leaks

	delete(c.entries, entry.keyHash)
	c.usedSize -= entry.size
}

func (c *cache) addEntry(entry *cacheEntry) {
	entry.next = c.first
	if c.first != nil {
		c.first.prev = entry
	}
	c.first = entry
	if c.last == nil {
		c.last = entry
	}

	c.entries[entry.keyHash] = entry
	c.usedSize += entry.size
}

func (c *cache) getEntry(keyHash KeyHash) *cacheEntry {
	c.gets.Add(1)

	c.lock.Lock()
	defer c.lock.Unlock()

	entry, ok := c.entries[keyHash]
	if ok {
		c.moveToFront(entry)
		return entry
	}

	return nil
}

func toFilename(keyHash KeyHash, mtime int64, expires int64, sequence uint64) (string, string) {
	var expiresStr string
	if expires == 0 {
		expiresStr = "+"
	} else {
		expiresStr = strconv.FormatInt(expires, 36)
	}
	shard := keyHash.toShard().String()
	name := keyHash.String() + "_" + strconv.FormatInt(mtime, 36) + "_" + expiresStr
	if sequence > 0 {
		name += "_" + strconv.FormatUint(sequence, 36)
	}
	return shard, name
}

func fromFilename(filename string) (keyHash KeyHash, mtime int64, expires int64, sequence uint64, err error) {
	parts := strings.Split(filename, "_")
	if len(parts) != 3 && len(parts) != 4 {
		return keyHash, mtime, expires, sequence, fmt.Errorf("%s should contain 4 underscore symbols but has %d", filename, len(parts))
	}

	keyHash, err = keyHashFromString(parts[0])
	if err != nil {
		return keyHash, mtime, expires, sequence, fmt.Errorf("failed to restore key hash from %s: %w", parts[0], err)
	}

	mtime, err = strconv.ParseInt(parts[1], 36, 64)
	if err != nil {
		return keyHash, mtime, expires, sequence, fmt.Errorf("failed to restore mtime from %s: %w", parts[1], err)
	}

	if parts[2] != "+" {
		expires, err = strconv.ParseInt(parts[2], 36, 64)
		if err != nil {
			return keyHash, mtime, expires, sequence, fmt.Errorf("failed to restore expires from %s: %w", parts[2], err)
		}
	}

	if len(parts) == 4 {
		sequence, err = strconv.ParseUint(parts[3], 36, 64)
		if err != nil {
			return keyHash, mtime, expires, sequence, fmt.Errorf("failed to restore sequence from %s: %w", parts[3], err)
		}
	}

	return keyHash, mtime, expires, sequence, nil
}

func (c *cache) putEntry(key Key, ttl time.Duration, keepOpen bool, filler Filler) (entry *cacheEntry, file *os.File, err error) {
	c.puts.Add(1)

	mtime := time.Now().UnixMilli()
	var expires int64
	if ttl != 0 {
		expires = mtime + ttl.Milliseconds()
	}
	sequence := c.sequence.Add(1)

	keyHash := key.ToHash()

	newPath := c.buildEntryPath(keyHash, mtime, expires, sequence)

	defer func() {
		if file != nil {
			if err != nil {
				// close & cleanup file but do not overwrite original error
				_ = file.Close()
				_ = os.Remove(newPath)
			} else {
				if !keepOpen {
					err = file.Close()
				}
			}
		}
	}()

	file, err = os.OpenFile(newPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, c.fileMode)
	if err != nil {
		return nil, file, err
	}

	var addedBytes int64
	addedBytes, err = filler.WriteCacheData(key, file)
	if err != nil {
		return nil, file, err
	}

	c.lock.RLock()
	item, exists := c.entries[keyHash]
	if exists {
		entry = item
	} else {
		entry = &cacheEntry{keyHash: keyHash}
	}
	c.lock.RUnlock()

	if exists {
		oldPath := c.buildEntryPath(keyHash, entry.mtime, entry.expires, entry.sequence)
		oldDelErr := os.Remove(oldPath)
		if oldDelErr != nil && !errors.Is(oldDelErr, os.ErrNotExist) {
			err = oldDelErr
			return nil, file, err
		}
	}

	c.lock.Lock()
	if exists {
		c.usedSize -= entry.size
		c.moveToFront(entry)
	} else {
		c.addEntry(entry)
	}
	c.usedSize += addedBytes
	c.lock.Unlock()

	entry.size = addedBytes
	entry.mtime = mtime
	entry.expires = expires
	entry.sequence = sequence

	// do eviction in background after put
	go c.evict()

	return entry, file, nil
}

func readDirWithoutSort(name string) ([]os.DirEntry, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	dirs, err := f.ReadDir(-1)
	_ = f.Close()

	return dirs, err
}

func (c *cache) loadEntries() []error {
	shardDirs, err := readDirWithoutSort(c.cacheDir)
	if err != nil {
		return []error{err}
	}

	workers := runtime.NumCPU()
	wg := sync.WaitGroup{}
	queue := make(chan string, workers)
	errsChan := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for shardDirPath := range queue {
				entries, err := readDirWithoutSort(shardDirPath)
				if err != nil {
					errsChan <- err
					continue
				}

				for _, entry := range entries {
					if !entry.IsDir() {
						path := filepath.Join(shardDirPath, entry.Name())

						keyHash, mtime, expires, sequence, err := fromFilename(entry.Name())
						if err != nil {
							errsChan <- fmt.Errorf("failed to restore meta from %s: %w", path, err)
							continue
						}

						info, err := entry.Info()
						if err != nil {
							errsChan <- fmt.Errorf("failed to restore size from %s: %w", path, err)
							continue
						}

						ce := &cacheEntry{
							keyHash:  keyHash,
							sequence: sequence,
							size:     info.Size(),
							mtime:    mtime,
							expires:  expires,
						}

						c.lock.Lock()
						existingEntry, exists := c.entries[ce.keyHash]
						if exists {
							c.lock.Unlock()
							newPath := c.buildEntryPath(existingEntry.keyHash, existingEntry.mtime, existingEntry.expires, existingEntry.sequence)
							if path != newPath {
								// Remove old file in case the key was written to by a normal put during loading.
								delErr := os.Remove(path)
								if delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
									err = delErr
								}
							}
							if err != nil {
								errsChan <- fmt.Errorf("failed to remove overwritten entry at %s: %w", path, err)
							}
						} else {
							c.addEntry(ce)
							c.lock.Unlock()
						}
					}
				}
			}
		}()
	}

	go func() {
		for _, shardDir := range shardDirs {
			if shardDir.IsDir() {
				shardDirPath := filepath.Join(c.cacheDir, shardDir.Name())
				queue <- shardDirPath
			}
		}
		close(queue)
	}()

	go func() {
		wg.Wait()
		close(errsChan)
	}()

	var errs []error
	for err := range errsChan {
		errs = append(errs, err)
	}

	return errs
}

func (c *cache) evict() {
	c.evictionLock.Lock()
	defer c.evictionLock.Unlock()

	if !c.evictionTime.IsZero() && time.Since(c.evictionTime) < c.evictionInterval {
		return
	}

	c.lock.RLock()

	start := time.Now()

	if c.usedSize > c.targetSize {

		var expired []*cacheEntry
		expiredSize := int64(0)

		var candidates []*cacheEntry
		candidatesSize := int64(0)

		for entry := c.last; entry != nil; entry = entry.prev {
			if entry.isExpired() {
				expired = append(expired, entry)
				expiredSize += entry.size
			} else {
				if c.usedSize-(expiredSize+candidatesSize) > c.targetSize {
					candidates = append(candidates, entry)
					candidatesSize += entry.size
				}
			}

			if c.usedSize-expiredSize < c.targetSize {
				break
			}
		}

		c.lock.RUnlock()
		c.lock.Lock()

		var deleted []*cacheEntry

		// remove all expired
		for _, entry := range expired {
			c.removeEntry(entry)
			c.evictions += 1

			deleted = append(deleted, entry)
		}

		// if target size is still not reached remove lru candidates until it is
		for _, entry := range candidates {
			if c.usedSize < c.targetSize {
				break
			}
			c.removeEntry(entry)
			c.evictions += 1

			deleted = append(deleted, entry)
		}

		// we can release the cache lock now and delete the actual files in "background"
		c.lock.Unlock()

		c.clearOrEvictDoingDeletes.Add(1)
		defer c.clearOrEvictDoingDeletes.Add(-1)

		for _, entry := range deleted {
			err := os.Remove(c.buildEntryPath(entry.keyHash, entry.mtime, entry.expires, entry.sequence))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				c.evictionErrors = append(c.evictionErrors, EvictionError{
					Time:  time.Now(),
					Error: err,
				})
				if len(c.evictionErrors) > 1000 {
					c.evictionErrors = c.evictionErrors[1:]
				}
			}
		}
	} else {
		c.lock.RUnlock()
	}

	c.evictionTime = time.Now()
	c.evictionDuration = time.Since(start)
}
