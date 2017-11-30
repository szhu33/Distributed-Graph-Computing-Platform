/* Modified from GO document:
https://golang.org/pkg/container/heap/
*/

// Package priorityqueue is a priority queue sorted by timestamp
package priorityqueue

import (
	"container/heap"
	"time"
)

// An Item is something we manage in a priority queue.
type Item struct {
	NodeID    int
	Timestamp time.Time
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	if pq[i].Timestamp.Equal(pq[j].Timestamp) {
		return pq[i].NodeID > pq[j].NodeID
	}
	return pq[i].Timestamp.After(pq[j].Timestamp)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push inserts an element to the priority queue.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop removes and return the top element in the priority queue
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Remove the entries with nodeID if exisits
func (pq *PriorityQueue) Remove(nodeID int) {
	for idx := 0; idx < len(*pq); idx++ {
		if (*pq)[idx].NodeID == nodeID {
			heap.Remove(pq, idx)
			return
		}
	}
}

// TryUpdate modifies the priority and value of an Item in the queue.
// Return true if the values are updated
// Return false if new item is inserted
func (pq *PriorityQueue) TryUpdate(nodeID int, newTS time.Time) bool {
	for idx := 0; idx < len(*pq); idx++ {
		if (*pq)[idx].NodeID == nodeID {
			pq.update((*pq)[idx], nodeID, newTS)
			return true
		}
	}
	newItem := &Item{
		NodeID:    nodeID,
		Timestamp: newTS,
	}
	pq.Push(newItem)
	return false
}

func (pq *PriorityQueue) update(item *Item, newNodeID int, newTS time.Time) {
	item.NodeID = newNodeID
	item.Timestamp = newTS
	heap.Fix(pq, item.index)
}

func (pq *PriorityQueue) Exists(nodeID int) bool {
	for idx := 0; idx < len(*pq); idx++ {
		if (*pq)[idx].NodeID == nodeID {
			return true
		}
	}
	return false
}

func (pq *PriorityQueue) GetTimestamp(nodeID int) time.Time {
	for idx := 0; idx < len(*pq); idx++ {
		if (*pq)[idx].NodeID == nodeID {
			return (*pq)[idx].Timestamp
		}
	}
	return time.Time{}
}
