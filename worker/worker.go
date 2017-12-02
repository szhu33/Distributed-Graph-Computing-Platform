package main

import (
	"bufio"
	"bytes"
	"cs425_mp4/api"
	"cs425_mp4/failure-detector"
	"cs425_mp4/protocol-buffer/superstep"
	"cs425_mp4/sdfs"
	"cs425_mp4/utility"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
)

const (
	workerPort       = "5888"
	workerNum        = 7
	masterworkerPort = "5558"
	nodeName         = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	START            = superstep.Superstep_START
	RUN              = superstep.Superstep_RUN
	ACK              = superstep.Superstep_ACK
	VOTETOHALT       = superstep.Superstep_VOTETOHALT
	localInputName   = "localFile.txt"
)

type vertexInfo struct {
	active       bool
	neighbors    []int
	value        api.VertexValue
	msgs         api.MessageValue
	nextStepMsgs api.MessageValue
}

var (
	vertex          map[int]vertexInfo
	stepcount       int
	myID            int
	masterID        uint32
	masterChan      chan superstep.Superstep
	masterMsg       superstep.Superstep
	workerIDs       []int //should range from 0-9
	datasetFilename string
	dataset         []byte
)

/* failure handling function */
func updateWorkerIDs() {
	aliveMembers := fd.MemberStatus()
	k := 0
	i := 0
	for k < workerNum {
		if aliveMembers[i] {
			workerIDs[k] = i
			k++
			i++
		}
	}
}

/* helper function */
func isInWorkerIDs(input int) bool {
	for _, elem := range workerIDs {
		if elem == input {
			return true
		}
	}
	return false
}

func updateVertex() {
	reader := bufio.NewReader(bytes.NewReader(dataset))
	fmt.Println()
	for {
		line, rdErr := reader.ReadString('\n')
		if rdErr == io.EOF {
			fmt.Println("Finished reading input")
			break
		} else if rdErr != nil {
			fmt.Println("Error read file!", rdErr.Error())
			return
		}
		words := strings.Fields(line)
		_, err := strconv.ParseInt(words[0], 10, 32)
		if err != nil {
			fmt.Println("ignore #")
			continue
		}

		// hash vertexID to vmID, if the vmID is not the worker, increment vertexID and hash it again until it is a valid worker
		from1, err := strconv.ParseInt(words[0], 10, 32)
		from := int(from1)
		dummyFromInt := from
		fromVm := int(util.HashToVMIdx(string(dummyFromInt)))
		for !isInWorkerIDs(fromVm) {
			dummyFromInt++
			fromVm = int(util.HashToVMIdx(string(dummyFromInt)))
		}
		to1, err := strconv.ParseInt(words[1], 10, 32)
		to := int(to1)
		dummyToInt := to
		toVm := int(util.HashToVMIdx(string(dummyToInt)))
		for !isInWorkerIDs(toVm) {
			dummyToInt++
			toVm = int(util.HashToVMIdx(string(dummyToInt)))
		}
		// fmt.Printf("fromvertex:%d, tovertex:%d, fromVm:%d, toVm:%d\n", from, to, fromVm, toVm)
		if (fromVm != myID) && (toVm != myID) {
			continue
		}
		if fromVm == myID {
			if _, ok := vertex[from]; ok {
				tempInfo := vertex[from]
				tempInfo.neighbors = append(tempInfo.neighbors, to)
				vertex[from] = tempInfo
			} else {
				nei := make([]int, 0)
				nei = append(nei, to)
				vertex[from] = vertexInfo{active: true, neighbors: nei}
			}
		} else {
			if _, ok := vertex[to]; ok {
				tempInfo := vertex[to]
				tempInfo.neighbors = append(tempInfo.neighbors, from)
				vertex[to] = tempInfo
			} else {
				nei := make([]int, 0)
				nei = append(nei, from)
				vertex[to] = vertexInfo{active: true, neighbors: nei}
			}
		}
	}
	fmt.Println("vertex result")
	fmt.Println(len(vertex))
	for key, val := range vertex {
		fmt.Printf("key:%d, active:%t, neighbors:%d\n", key, val.active, val.neighbors)
	}
}

func initialize() {
	stepcount = 0
	updateWorkerIDs()
	// TODO: get file from sdfs and put vertex into map
	//dataset = mp3.GetFile(datasetFilename)
	updateVertex()
}

/* worker related function */
func computeAllVertex() {

}

/* master related function */
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

		if masterMsg.GetSource() != masterID {
			masterID = masterMsg.GetSource()
		}

		if masterMsg.GetCommand() == START {
			go initialize()
		}
		masterChan <- masterMsg
	}
}

func main() {
	//TODO: get myid from hostname
	go sdfs.Start()
	myID = util.GetIDFromHostname()
	vertex = make(map[int]vertexInfo)
	go listenMaster()
	// testing
	masterID = 10
	initialize()
	for {

	}
}
