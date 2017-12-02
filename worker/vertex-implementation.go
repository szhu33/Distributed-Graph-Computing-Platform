package main

func (v VertexPageRank) GetValue() float64 {
	return v.value
}

func (v *VertexPageRank) MutableValue(newVal float64) {
	v.value = newVal
}

func (v *VertexPageRank) Superstep() int {
	return stepcount
}

func (v *VertexPageRank) VoteToHalt() {
	// send halt message to master
}
