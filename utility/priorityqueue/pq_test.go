package priorityqueue

import (
	"container/heap"
	"fmt"
	"testing"
	"time"
)

// This example creates a PriorityQueue with some items, adds and manipulates an item,
// and then removes the items in priority order.
func TestPq(t *testing.T) {

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	ts := time.Now()
	pq := make(PriorityQueue, 10)
	for idx := 0; idx < 10; idx++ {
		pq[idx] = &Item{
			NodeID:    idx,
			Timestamp: ts,
			index:     idx,
		}
	}

	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	item := &Item{
		NodeID:    101,
		Timestamp: time.Now(),
	}
	heap.Push(&pq, item)
	pq.TryUpdate(101, time.Now())
	pq.TryUpdate(100, item.Timestamp)
	pq.TryUpdate(100, item.Timestamp)
	pq.Remove(0)
	pq.Remove(-1)

	fmt.Println(pq.Exists(1000)) // false
	fmt.Println(pq.Exists(100))  // True

	if pq.Len() != 11 {
		t.Errorf("Failed.")
	}

	for idx := 0; idx < pq.Len(); idx++ {
		fmt.Printf("%d:%s\n ", pq[idx].NodeID, pq[idx].Timestamp.String())
	}
	fmt.Println()
	// Take the items out; they arrive in decreasing priority order.
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		fmt.Printf("%d:%s\n ", item.NodeID, item.Timestamp.String())
	}

	if pq.Len() != 0 {
		t.Errorf("Failed.")
	}
}
