package main

import (
	"cs425_mp4/api"
	"cs425_mp4/protocol-buffer/worker-worker"
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
	if v.Superstep() == 0 {
		fmt.Println("stepcount0!")
	}
	if v.Superstep() >= 1 {
		sum := 0.0
		for {
			val, isEnd := msgs.Next()
			fmt.Println("stepcount:", stepcount, "Msg vertexID: ", v.Id, "Msg val:", val)
			if isEnd {
				break
			}
			sum += val
		}
		newVal := 0.15/float64(NumVertices()) + 0.85*sum
		v.MutableValue(newVal)
		fmt.Println("superstep", stepcount, "vertex:", v.Vertex_id(), "sum:", sum, "newVal:", newVal, "numVertices:", NumVertices())
	}

	if v.Superstep() < 30 {
		neighbors := v.GetOutEdge()
		n := float64(len(neighbors))
		fmt.Println("neighbor nums", n)
		for _, edge := range neighbors {
			if v.Superstep() == 0 {
				fmt.Println("send out msg when stepcount ==0")
			}
			v.SendMessageTo(edge.dest, v.GetValue()/n)
			fmt.Println("superstep", stepcount, "send out msg from:", v.Vertex_id(), "Send to:", edge.dest, "val:", v.GetValue()/n)
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

func (q *vertexMsgQ) Next() (float64, bool) {
	fmt.Println("index:", q.index, "len:", len(q.queue))
	if q.index >= len(q.queue) {
		return 0.0, true
	}

	val := q.queue[q.index].GetMsgValue()
	q.index++
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

	original, ok := msgV.(float64)
	if ok {
		newWorkerMsg := &workerpb.Worker{FromVertex: uint64(v.Id), Stepcount: stepcount, ToVertex: uint64(destVertexID), MsgValue: original}
		sendToWorker(newWorkerMsg)
	} else {
		fmt.Println("Failed to convert to float.")
	}

}

func (v VertexPageRank) GetOutEdge() []edgeT {
	return vertices[v.Vertex_id()].neighbors
}
