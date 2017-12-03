package main

import (
	"bytes"
	"cs425_mp4/api"
	"cs425_mp4/protocol-buffer/worker-worker"
	"encoding/gob"
	"fmt"
)

// VertexPageRank is actual implementation of vertex in PageRank
type VertexPageRank struct {
	api.Vertex
	Value float64
	edges map[int]float64
	Id    int
}

// Compute is the client implementation
// return the active status of the vertex
func (v *VertexPageRank) Compute(msgs api.MessageIterator) bool {
	if v.Superstep() >= 1 {
		sum := 0.0
		for {
			val, isEnd := msgs.Next()
			fmt.Println("isEnd:", isEnd, "val:", val)
			if isEnd {
				break
			}
			sum += val.(float64)
		}
		newVal := 0.15/float64(NumVertices()) + 0.85*sum
		v.MutableValue(newVal)
		fmt.Println("superstep", stepcount, "from:", v.Vertex_id(), "sum:", sum, "newVal:", newVal, "numVertices:", NumVertices())
	}

	if v.Superstep() < 30 {
		neighbors := v.GetOutEdge()
		n := float64(len(neighbors))
		for _, edge := range neighbors {
			v.SendMessageTo(edge.dest, v.GetValue()/n)
			fmt.Println("superstep", stepcount, "from:", v.Vertex_id(), "Send to:", edge.dest, "val:", v.GetValue()/n)
		}
	} else {
		fmt.Println("Halt vertex:", v.Vertex_id())
		return v.VoteToHalt()
	}
	return true
}

/* Actual implementation in worker*/
type vertexMsgQ struct {
	queue []*workerpb.Worker
	index int
}

func (q *vertexMsgQ) Next() (interface{}, bool) {
	fmt.Println("index:", q.index, "len:", len(q.queue))
	if q.index >= len(q.queue) {
		return 0.0, true
	}
	rd := bytes.NewReader(q.queue[q.index].GetMsgValue())
	dec := gob.NewDecoder(rd)
	q.index++
	var val float64
	err := dec.Decode(&val)
	if err != nil {
		fmt.Println("decode error:", err.Error())
		return val, true
	}
	return val, false
}

// GetValue returns value of the vertex
func (v VertexPageRank) GetValue() float64 {
	return v.Value
}

// MutableValue changes the value in the vertex
func (v *VertexPageRank) MutableValue(newVal float64) {
	v.Value = newVal
}

// Superstep returns the step count
func (v VertexPageRank) Superstep() uint64 {
	return stepcount
}

// Vertex_id returns the id of the vertex
func (v VertexPageRank) Vertex_id() int {
	return v.Id
}

// VoteToHalt halt the vertex
func (v VertexPageRank) VoteToHalt() bool {
	// send halt message to master
	return false
}

func (v VertexPageRank) SendMessageTo(destVertexID int, msgV interface{}) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b) // write to buffer b

	original, ok := msgV.(float64)
	if ok {
		err := enc.Encode(original)
		if err != nil {
			fmt.Println("Cannot encode msg value when sending msg")
			return
		}
		newWorkerMsg := &workerpb.Worker{FromVertex: uint64(v.Id), Stepcount: stepcount, ToVertex: uint64(destVertexID), MsgValue: b.Bytes()}
		sendToWorker(newWorkerMsg)
	}

}

func (v VertexPageRank) GetOutEdge() []edgeT {
	return vertices[v.Vertex_id()].neighbors
}
