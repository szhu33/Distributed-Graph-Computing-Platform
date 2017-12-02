package main

import (
	"cs425_mp4/api"
	"fmt"
)

// VertexPageRank is actual implementation of vertex in PageRank
type VertexPageRank struct {
	api.Vertex
	value float64
	edges map[int]float64
	id    int
}

func (v *VertexPageRank) Compute(msgs api.MessageIterator) {
	for {
		currVal := v.GetValue()
		val, isEnd := msgs.Next()
		if isEnd {
			v.VoteToHalt()
			break
		}
		fmt.Println(val, currVal)
	}
}
