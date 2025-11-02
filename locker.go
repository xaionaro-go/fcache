package fcache

import (
	"sync"
	"sync/atomic"
)

// Locker provides a key based locking mechanism
type Locker[KH KeyHash] struct {
	holderPool sync.Pool
	mu         sync.Mutex
	locks      map[KH]*lockHolder
}

type lockHolder struct {
	lock      sync.RWMutex
	users     uint32 // users is the number of users currently interacting with the lock
	upgrading atomic.Int32
}

// NewLocker creates a new Locker ready to be used
func NewLocker[KH KeyHash]() *Locker[KH] {
	return &Locker[KH]{
		holderPool: sync.Pool{New: func() any { return new(lockHolder) }},
		locks:      make(map[KH]*lockHolder),
	}
}

func (l *Locker[KH]) prepareToLock(key KH) *lockHolder {
	l.mu.Lock()

	holder, exists := l.locks[key]
	if !exists {
		holder = l.holderPool.Get().(*lockHolder)
		l.locks[key] = holder
	}
	holder.users += 1

	l.mu.Unlock()

	return holder
}

// Lock locks a mutex with the given key for writing. If no mutex for that key exists it is created.
func (l *Locker[KH]) Lock(key KH) {
	holder := l.prepareToLock(key)
lock:
	holder.lock.Lock()
	// Upgrade gets priority, retry lock if upgrade is in progress.
	if holder.upgrading.Load() > 0 {
		holder.lock.Unlock()
		goto lock
	}
}

// RLock locks a mutex with the given key for reading. If no mutex for that key exists it is created.
func (l *Locker[KH]) RLock(key KH) {
	holder := l.prepareToLock(key)
	holder.lock.RLock()
}

// Upgrade converts a read lock to a write lock for the given key.
// If the upgrade would deadlock or is not possible it returns false.
// Inspired by https://github.com/kawasin73/umutex
func (l *Locker[KH]) Upgrade(key KH) (success bool) {
	l.mu.Lock()
	holder, exists := l.locks[key]
	if exists {
		l.mu.Unlock()

		success = holder.upgrading.Add(1) == 1
		if success {
			holder.lock.RUnlock()
			holder.lock.Lock()
		}
		holder.upgrading.Add(-1)
	} else {
		l.mu.Unlock()
	}
	return success
}

func (l *Locker[KH]) prepareToUnlock(key KH) *sync.RWMutex {
	l.mu.Lock()

	holder, exists := l.locks[key]
	if !exists {
		l.mu.Unlock()
		return nil
	}

	holder.users -= 1      // about to unlock so we can decrement the number of users for this lock
	if holder.users == 0 { // if this was the last user we also remove it from the map
		delete(l.locks, key)
		l.holderPool.Put(holder) // potential early reuse by another key before final unlock should not be a problem
	}

	l.mu.Unlock()

	return &holder.lock
}

// Unlock unlocks the mutex with the given key
func (l *Locker[KH]) Unlock(key KH) {
	lock := l.prepareToUnlock(key)
	if lock != nil {
		lock.Unlock()
	}
}

// RUnlock unlocks the mutex with the given key
func (l *Locker[KH]) RUnlock(key KH) {
	lock := l.prepareToUnlock(key)
	if lock != nil {
		lock.RUnlock()
	}
}

// Size returns the number of internal locks
func (l *Locker[KH]) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.locks)
}
