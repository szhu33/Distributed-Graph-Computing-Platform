// This example demonstrates an integer heap built using the heap interface.
package maxheap

import (
	"cs425_mp4/protocol-buffer/superstep"
)

// An IntHeap is a min-heap of ints.
type VertexHeap []*superstep.Vertex

func (h VertexHeap) Len() int           { return len(h) }
func (h VertexHeap) Less(i, j int) bool { return h[i].GetValue() < h[j].GetValue() }
func (h VertexHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *VertexHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*superstep.Vertex))
}

func (h *VertexHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
