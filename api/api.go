package api

// MessageIterator defins a iterator
type MessageIterator interface {
	Next() (float64, bool)
}

// Vertex defines functions needed for Vertex class
type Vertex interface {

	// Compute is defined by user application
	Compute(msgs MessageIterator)

	Vertex_id() int
	Superstep() int

	GetValue() interface{}
	MutableValue()

	SendMessageTo()
	VoteToHalt()
}
