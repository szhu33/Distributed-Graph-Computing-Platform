package sava

type VertexValue interface{}
type EdgeValue interface{}
type MessageValue interface{}

type MessageIterator interface {
	Next() (MessageValue, bool)
}

type Vertex interface {
	// Compute is defined by user application
	Compute(msgs MessageIterator)

	Vertex_id() int
	Superstep() int

	GetValue() VertexValue
	MutableValue() *VertexValue
	GetOutEdgeIterator()

	SendMessageTo()
	VoteToHalt()
}

type VertexReal struct {
	id  int
	val VertexValue

	neighbors map[int]EdgeValue
}
