package sava

type VertexValue interface{}
type EdgeValue interface{}
type MessageValue interface{}

type Vertex interface {
	Compute()
	SendMessageTo()
	VoteToHalt()
	GetValue() VertexValue
	vertex_id() int
}
