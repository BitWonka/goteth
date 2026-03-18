package utils

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWaitUntilInactiveBarrier verifies that WaitUntilInactive can be used as
// a barrier to ensure a previous task completes before the next one reads
// shared state — the pattern used to fix the ManualReward race condition
// in ProcessStateTransitionMetrics (see github.com/migalabs/goteth/issues/242).
func TestWaitUntilInactiveBarrier(t *testing.T) {
	book := NewRoutineBook(4, "test")

	// Shared mutable state simulating block.ManualReward
	var sharedReward atomic.Int64

	const expectedReward = 81413876 // correct value after full accumulation
	const partialReward = 47798640  // value if read mid-accumulation

	var wg sync.WaitGroup

	// Simulate epoch N processing: slowly writes ManualReward
	wg.Add(1)
	go func() {
		defer wg.Done()
		key := "epoch=10"
		book.Acquire(key)
		defer book.FreePage(key)

		// Simulate incremental accumulation (ProcessAttestations)
		sharedReward.Store(int64(partialReward))
		time.Sleep(50 * time.Millisecond) // simulate processing time
		sharedReward.Store(int64(expectedReward))
	}()

	// Give epoch N a head start to acquire its page
	time.Sleep(10 * time.Millisecond)

	// Simulate epoch N+1 processing: reads ManualReward with barrier
	wg.Add(1)
	go func() {
		defer wg.Done()
		key := "epoch=11"
		book.Acquire(key)
		defer book.FreePage(key)

		// Barrier: wait for previous epoch to complete
		book.WaitUntilInactive("epoch=10")

		// Read shared state — should see the final value
		got := sharedReward.Load()
		if got != expectedReward {
			t.Errorf("ManualReward = %d, want %d (read partial value, barrier failed)", got, expectedReward)
		}
	}()

	wg.Wait()
}

// TestWaitUntilInactiveNeverRegistered verifies that WaitUntilInactive
// returns immediately when the key was never registered (e.g. epoch N-1
// was before initSlot and was never processed).
func TestWaitUntilInactiveNeverRegistered(t *testing.T) {
	book := NewRoutineBook(4, "test")

	done := make(chan bool, 1)
	go func() {
		book.WaitUntilInactive("epoch=999")
		done <- true
	}()

	select {
	case <-done:
		// OK — returned immediately
	case <-time.After(3 * time.Second):
		t.Fatal("WaitUntilInactive blocked on never-registered key")
	}
}
