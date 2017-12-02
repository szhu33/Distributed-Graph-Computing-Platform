package main

import (
	"bytes"
	"cs425_mp4/api"
	"cs425_mp4/protocol-buffer/worker-worker"
	"encoding/gob"
	"fmt"

	"github.com/golang/protobuf/proto"
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

/* Actual implementation in worker*/

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
		newWorkerMsg := &workerpb.Worker{Source: uint32(myID), Stepcount: stepcount, MsgValue: b.Bytes()}
		pb, err := proto.Marshal(newWorkerMsg)
		if err != nil {
			fmt.Println("Error when marshal worker-worker message.", err.Error())
		}
		sendToWorker(destVertexID, pb)

	}

}
