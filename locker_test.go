package fcache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func expectNotDone(t *testing.T, chDone chan struct{}, msg string) {
	t.Helper()
	select {
	case <-chDone:
		t.Fatal(msg)
	case <-time.After(1 * time.Millisecond):
		// not done
	}
}

func expectDone(t *testing.T, chDone chan struct{}, msg string) {
	t.Helper()
	select {
	case <-chDone:
		// done
	case <-time.After(10 * time.Millisecond):
		t.Fatal(msg)
	}
}

func TestLockerLock(t *testing.T) {
	l := NewLocker()
	l.Lock(Key("1").ToHash())
	holder := l.locks[Key("1").ToHash()]

	if holder.users != 1 {
		t.Fatalf("expected users to be 1, got :%d", holder.users)
	}
	if l.Size() != 1 {
		t.Fatalf("expected size to be 1, got: %d", l.Size())
	}

	chDone := make(chan struct{})
	go func() {
		l.Lock(Key("1").ToHash())
		close(chDone)
	}()

	chWaiting := make(chan struct{})
	go func() {
		for range time.Tick(1 * time.Millisecond) {
			if holder.users == 2 {
				close(chWaiting)
				break
			}
		}
	}()

	expectDone(t, chWaiting, "timed out waiting for lock users to be incremented")

	if l.Size() != 1 {
		t.Fatalf("expected size to be 1, got: %d", l.Size())
	}

	expectNotDone(t, chDone, "lock should not have returned while it was still held")

	l.Unlock(Key("1").ToHash())

	expectDone(t, chDone, "lock should have completed")

	if holder.users != 1 {
		t.Fatalf("expected users to be 1, got: %d", holder.users)
	}
	if l.Size() != 1 {
		t.Fatalf("expected size to be 1, got: %d", l.Size())
	}
}

func TestLockerUnlock(t *testing.T) {
	l := NewLocker()

	l.Lock(Key("1").ToHash())
	l.Unlock(Key("1").ToHash())

	if l.Size() != 0 {
		t.Fatalf("expected size to be 0, got: %d", l.Size())
	}

	chDone := make(chan struct{})
	go func() {
		l.Lock(Key("1").ToHash())
		close(chDone)
	}()

	expectDone(t, chDone, "lock should not be blocked")

	if l.Size() != 1 {
		t.Fatalf("expected size to be 1, got: %d", l.Size())
	}
}

func TestLockerUpgrade(t *testing.T) {
	l := NewLocker()
	chDone := make(chan struct{})

	l.RLock(Key("1").ToHash())
	l.RLock(Key("1").ToHash())
	go func() {
		l.Upgrade(Key("1").ToHash())
		chDone <- struct{}{}
	}()
	expectNotDone(t, chDone, "RLock prevents Upgrade")

	// double Upgrade dead-locks.
	if l.Upgrade(Key("1").ToHash()) {
		t.Error("Upgrade dead-lock")
	}

	l.RUnlock(Key("1").ToHash())
	expectDone(t, chDone, "RUnlock enables Upgrade")

	go func() {
		l.RLock(Key("1").ToHash())
		chDone <- struct{}{}
	}()
	expectNotDone(t, chDone, "Upgraded mutex prevents RLock")

	l.Unlock(Key("1").ToHash())
	expectDone(t, chDone, "Unlock enables RLock")

	// Upgrade is given priority to Lock.
	go func() {
		l.Lock(Key("1").ToHash())
		chDone <- struct{}{}
	}()
	expectNotDone(t, chDone, "RLock prevents Lock")

	if !l.Upgrade(Key("1").ToHash()) {
		t.Error("failed to Upgrade")
	}

	expectNotDone(t, chDone, "Upgrade is given priority to Lock")

	l.Unlock(Key("1").ToHash())

	expectDone(t, chDone, "Unlock enables Lock")
}

func TestLockerLockTwoKeys(t *testing.T) {
	l := NewLocker()
	l.Lock(Key("1").ToHash())

	if l.Size() != 1 {
		t.Fatalf("expected size to be 1, got: %d", l.Size())
	}

	l.Lock(Key("2").ToHash())

	if l.Size() != 2 {
		t.Fatalf("expected size to be 2, got: %d", l.Size())
	}

	l.Unlock(Key("1").ToHash())

	if l.Size() != 1 {
		t.Fatalf("expected size to be 1, got: %d", l.Size())
	}

	l.Unlock(Key("2").ToHash())

	if l.Size() != 0 {
		t.Fatalf("expected size to be 0, got: %d", l.Size())
	}
}

func TestLockerConcurrency(t *testing.T) {
	l := NewLocker()

	expectedWrites := 0
	writes := 0

	var wg sync.WaitGroup
	for i := 0; i <= 1000; i++ {
		wg.Add(1)
		r := rand.Intn(2)
		if r == 0 {
			expectedWrites += 1
		}
		go func(r int) {
			defer wg.Done()
			if r == 0 {
				l.Lock(Key("1").ToHash())
				writes += 1
				defer l.Unlock(Key("1").ToHash())
			} else {
				l.RLock(Key("1").ToHash())
				defer l.RUnlock(Key("1").ToHash())
			}
			s := time.Now()
			for time.Now().Sub(s) < 1*time.Millisecond {
				// busy waiting, with time.Sleep it takes way longer for some reason
			}
		}(r)
	}

	chDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(chDone)
	}()

	select {
	case <-chDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for locks to complete")
	}

	if holder, exists := l.locks[Key("1").ToHash()]; exists {
		t.Fatalf("lock should not exist: %v", holder)
	}

	if expectedWrites != writes {
		t.Fatalf("expected %d writes but got %d", expectedWrites, writes)
	}
}

func BenchmarkLocker(b *testing.B) {
	l := NewLocker()
	for i := 0; i < b.N; i++ {
		l.Lock(Key("1").ToHash())
		l.Unlock(Key("1").ToHash())
	}
}

func BenchmarkLockerParallel(b *testing.B) {
	l := NewLocker()
	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Lock(Key("1").ToHash())
			l.Unlock(Key("1").ToHash())
		}
	})
}

func BenchmarkLockerMoreKeys(b *testing.B) {
	l := NewLocker()
	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := uint64(rand.Intn(64))
			l.Lock(Key(fmt.Sprintf("%d", k)).ToHash())
			time.Sleep(5 * time.Millisecond)
			l.Unlock(Key(fmt.Sprintf("%d", k)).ToHash())
		}
	})
}
