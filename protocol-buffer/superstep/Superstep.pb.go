// Code generated by protoc-gen-go. DO NOT EDIT.
// source: Superstep.proto

/*
Package superstep is a generated protocol buffer package.

It is generated from these files:
	Superstep.proto

It has these top-level messages:
	Superstep
*/
package superstep

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Superstep_Command int32

const (
	Superstep_START      Superstep_Command = 0
	Superstep_RUN        Superstep_Command = 1
	Superstep_ACK        Superstep_Command = 2
	Superstep_VOTETOHALT Superstep_Command = 3
)

var Superstep_Command_name = map[int32]string{
	0: "START",
	1: "RUN",
	2: "ACK",
	3: "VOTETOHALT",
}
var Superstep_Command_value = map[string]int32{
	"START":      0,
	"RUN":        1,
	"ACK":        2,
	"VOTETOHALT": 3,
}

func (x Superstep_Command) String() string {
	return proto.EnumName(Superstep_Command_name, int32(x))
}
func (Superstep_Command) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0, 0} }

type Superstep struct {
	Source    uint32            `protobuf:"varint,1,opt,name=source" json:"source,omitempty"`
	Command   Superstep_Command `protobuf:"varint,2,opt,name=command,enum=superstep.Superstep_Command" json:"command,omitempty"`
	Stepcount uint64            `protobuf:"varint,3,opt,name=stepcount" json:"stepcount,omitempty"`
}

func (m *Superstep) Reset()                    { *m = Superstep{} }
func (m *Superstep) String() string            { return proto.CompactTextString(m) }
func (*Superstep) ProtoMessage()               {}
func (*Superstep) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Superstep) GetSource() uint32 {
	if m != nil {
		return m.Source
	}
	return 0
}

func (m *Superstep) GetCommand() Superstep_Command {
	if m != nil {
		return m.Command
	}
	return Superstep_START
}

func (m *Superstep) GetStepcount() uint64 {
	if m != nil {
		return m.Stepcount
	}
	return 0
}

func init() {
	proto.RegisterType((*Superstep)(nil), "superstep.Superstep")
	proto.RegisterEnum("superstep.Superstep_Command", Superstep_Command_name, Superstep_Command_value)
}

func init() { proto.RegisterFile("Superstep.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 177 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x0f, 0x2e, 0x2d, 0x48,
	0x2d, 0x2a, 0x2e, 0x49, 0x2d, 0xd0, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x2c, 0x86, 0x09,
	0x28, 0x6d, 0x64, 0xe4, 0xe2, 0x84, 0x4b, 0x0b, 0x89, 0x71, 0xb1, 0x15, 0xe7, 0x97, 0x16, 0x25,
	0xa7, 0x4a, 0x30, 0x2a, 0x30, 0x6a, 0xf0, 0x06, 0x41, 0x79, 0x42, 0x66, 0x5c, 0xec, 0xc9, 0xf9,
	0xb9, 0xb9, 0x89, 0x79, 0x29, 0x12, 0x4c, 0x0a, 0x8c, 0x1a, 0x7c, 0x46, 0x32, 0x7a, 0x70, 0x23,
	0xf4, 0x10, 0xa6, 0x3b, 0x43, 0xd4, 0x04, 0xc1, 0x14, 0x0b, 0xc9, 0x70, 0x71, 0x82, 0x24, 0x92,
	0xf3, 0x4b, 0xf3, 0x4a, 0x24, 0x98, 0x15, 0x18, 0x35, 0x58, 0x82, 0x10, 0x02, 0x4a, 0x66, 0x5c,
	0xec, 0x50, 0x1d, 0x42, 0x9c, 0x5c, 0xac, 0xc1, 0x21, 0x8e, 0x41, 0x21, 0x02, 0x0c, 0x42, 0xec,
	0x5c, 0xcc, 0x41, 0xa1, 0x7e, 0x02, 0x8c, 0x20, 0x86, 0xa3, 0xb3, 0xb7, 0x00, 0x93, 0x10, 0x1f,
	0x17, 0x57, 0x98, 0x7f, 0x88, 0x6b, 0x88, 0xbf, 0x87, 0xa3, 0x4f, 0x88, 0x00, 0x73, 0x12, 0x1b,
	0xd8, 0x17, 0xc6, 0x80, 0x00, 0x00, 0x00, 0xff, 0xff, 0x48, 0x47, 0xdd, 0xcd, 0xd8, 0x00, 0x00,
	0x00,
}
