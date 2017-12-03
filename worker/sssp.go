package main

import "cs425_mp4/api"

// VertexPageRank is actual implementation of vertex in PageRank
type VertexSSSP struct {
	api.Vertex
	value float64
	edges map[int]float64
	id    int
}

// GetValue returns value of the vertex
func (v VertexSSSP) GetValue() float64 {
	return v.value
}
