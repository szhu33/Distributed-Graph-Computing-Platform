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
	value float64
	edges map[int]float64
	id    int
}

// Compute is the client implementation
func (v *VertexPageRank) Compute(msgs api.MessageIterator) {
	if v.Superstep() >= 1 {
		sum := 0.0
		for {
			val, isEnd := msgs.Next()
			if isEnd {
				break
			}
			sum += val.(float64)
			v.MutableValue(0.15/float64(NumVertices()) + 0.85*sum)
			fmt.Println(sum)
		}
	}

	if v.Superstep() < 30 {
		neighbors := v.GetOutEdge()
		n := float64(len(neighbors))
		for _, edge := range neighbors {
			v.SendMessageTo(edge.dest, v.GetValue()/n)
		}
	} else {
		v.VoteToHalt()
	}
}

/* Actual implementation in worker*/
type vertexMsgQ struct {
	queue []*workerpb.Worker
	index int
}

func (q vertexMsgQ) Next() (interface{}, bool) {
	if (q.index + 1) >= len(q.queue) {
		return 0, true
	}
	rd := bytes.NewReader(q.queue[q.index].GetMsgValue())
	dec := gob.NewDecoder(rd)
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
	return v.value
}

// MutableValue changes the value in the vertex
func (v *VertexPageRank) MutableValue(newVal float64) {
	v.value = newVal
}

// Superstep returns the step count
func (v VertexPageRank) Superstep() uint64 {
	return stepcount
}

// Vertex_id returns the id of the vertex
func (v VertexPageRank) Vertex_id() int {
	return v.id
}

// VoteToHalt halt the vertex
func (v VertexPageRank) VoteToHalt() {
	// send halt message to master
	temp := vertices[v.id]
	temp.active = false
	vertices[v.id] = temp
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
		newWorkerMsg := &workerpb.Worker{FromVertex: uint64(v.id), Stepcount: stepcount, ToVertex: uint64(destVertexID), MsgValue: b.Bytes()}
		sendToWorker(newWorkerMsg)

	}

}

func (v VertexPageRank) GetOutEdge() []edgeT {
	return vertices[v.Vertex_id()].neighbors
}
