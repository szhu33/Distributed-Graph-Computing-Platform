package main

import (
	"bytes"
	"cs425_mp4/common-library"
	"cs425_mp4/protocol-buffer/superstep"
	"fmt"
	"io"
	"net"

	"github.com/golang/protobuf/proto"
)

const (
	workerPort       = "5888"
	masterworkerPort = "5558"
	nodeName         = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	START            = superstep.Superstep_START
	RUN              = superstep.Superstep_RUN
	ACK              = superstep.Superstep_ACK
	VOTETOHALT       = superstep.Superstep_VOTETOHALT
)

type vertexInfo struct {
	active       bool
	neighbors    []int
	value        sava.VertexValue
	msgs         sava.MessageValue
	nextStepMsgs sava.MessageValue
}

var (
	vertex      map[int]vertexInfo
	stepcount   int
	masterNext  chan superstep.Superstep
	masterStart chan superstep.Superstep
	masterMsg   superstep.Superstep
)

func initialize() {
	stepcount = 0
	// TODO: get file from sdfs
}

func listenMaster() {
	for {
		ln, err := net.Listen("tcp", masterworkerPort)
		if err != nil {
			fmt.Println("cannot listen on port")
			return
		}
		defer ln.Close()

		var buf bytes.Buffer

		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("error occured!", err.Error())
			return
		}
		defer conn.Close()

		_, err = io.Copy(&buf, conn)
		if err != nil {
			fmt.Println("error occured!", err.Error())
			return
		}

		proto.Unmarshal(buf.Bytes(), &masterMsg)

		if masterMsg.GetCommand() == START {
			go initialize()
		}
	}
}

func main() {
	go listenMaster()
}
