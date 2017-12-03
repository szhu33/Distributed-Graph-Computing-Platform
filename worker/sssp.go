package main

import (
	"cs425_mp4/api"
	"cs425_mp4/protocol-buffer/worker-worker"
	"fmt"
	"math"
)

// VertexSSSP is actual implementation of vertex in PageRank
type VertexSSSP struct {
	api.Vertex
	Value float64
	edges map[int]float64
	Id    int
}

// Compute is the client implementation
// return the active status of the vertex
func (v *VertexSSSP) Compute(msgs api.MessageIterator) bool {
	var mindist float64
	if v.Id == SSSP_source {
		mindist = 0
	} else {
		mindist = math.Inf(+1)
	}
	for {
		val, isEnd := msgs.Next()
		if isEnd {
			break
		}
		if val < mindist {
			mindist = val
		}
	}
	if mindist < v.Value {
		neighbors := v.GetOutEdge()
		v.MutableValue(float64(mindist))
		for _, edge := range neighbors {
			v.SendMessageTo(edge.dest, float64(mindist)+edge.value)
		}
	}
	return true
}

/* Actual implementation in worker*/

// GetValue returns value of the vertex
func (v VertexSSSP) GetValue() float64 {
	return v.Value
}

// MutableValue changes the value in the vertex
func (v *VertexSSSP) MutableValue(newVal float64) {
	v.Value = newVal
}

// Superstep returns the step count
func (v VertexSSSP) Superstep() uint64 {
	return stepcount
}

// Vertex_id returns the id of the vertex
func (v VertexSSSP) Vertex_id() int {
	return v.Id
}

// VoteToHalt halt the vertex
func (v VertexSSSP) VoteToHalt() bool {
	// send halt message to master
	return false
}

func (v VertexSSSP) SendMessageTo(destVertexID int, msgV interface{}) {

	original, ok := msgV.(float64)
	if ok {
		newWorkerMsg := &workerpb.Worker{FromVertex: uint64(v.Id), Stepcount: stepcount, ToVertex: uint64(destVertexID), MsgValue: original}
		sendToWorker(newWorkerMsg)
	} else {
		fmt.Println("Failed to convert to float.")
	}

}

func (v VertexSSSP) GetOutEdge() []edgeT {
	return neighborMap[v.Vertex_id()]
}
