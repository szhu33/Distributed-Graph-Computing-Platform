package main

import (
	"bufio"
	"bytes"
	fd "cs425_mp4/failure-detector"
	ssproto "cs425_mp4/protocol-buffer/superstep"
	"cs425_mp4/sdfs"
	"cs425_mp4/utility"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
)

const (
	workerPort       = ":5888"
	totalNodes       = 10
	workerNum        = 7
	masterworkerPort = ":5558"
	nodeName         = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	START            = ssproto.Superstep_START
	RUN              = ssproto.Superstep_RUN
	ACK              = ssproto.Superstep_ACK
	VOTETOHALT       = ssproto.Superstep_VOTETOHALT
	localInputName   = "localFile.txt"
)

type vertexInfo struct {
	active    bool
	neighbors []edgeT
	value     float64
	msgs      []float64
	prevMsgs  []float64
	VertexPageRank
}

type edgeT struct {
	dest  int
	value float64
}

var (
	vertices        map[int]vertexInfo
	stepcount       int
	myID            int
	masterID        uint32
	masterChan      chan ssproto.Superstep
	masterMsg       ssproto.Superstep
	workerIDs       [totalNodes]int //should range from 0-9
	datasetFilename string
	dataset         []byte
)

/* failure handling function */
func updateWorkerIDs() {
	aliveMembers := fd.MemberStatus()
	fmt.Println(aliveMembers)
	k := 0
	i := 0
	for k < workerNum {
		if aliveMembers[i] {
			workerIDs[k] = i
			k++
		}
		i++
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
		toVM := int(util.HashToVMIdx(string(dummyToInt)))
		for !isInWorkerIDs(toVM) {
			dummyToInt++
			toVM = int(util.HashToVMIdx(string(dummyToInt)))
		}
		// fmt.Printf("fromvertex:%d, tovertex:%d, fromVm:%d, toVm:%d\n", from, to, fromVm, toVm)
		if (fromVm != myID) && (toVM != myID) {
			continue
		}
		if fromVm == myID {
			if _, ok := vertices[from]; ok {
				tempInfo := vertices[from]
				tempInfo.neighbors = append(tempInfo.neighbors, edgeT{dest: to, value: 1})
				vertices[from] = tempInfo
			} else {
				nei := make([]edgeT, 0)
				nei = append(nei, edgeT{dest: to, value: 1})
				vertices[from] = vertexInfo{active: true, neighbors: nei}
			}
		} else {
			if _, ok := vertices[to]; ok {
				tempInfo := vertices[to]
				tempInfo.neighbors = append(tempInfo.neighbors, edgeT{dest: from, value: 1})
				vertices[to] = tempInfo
			} else {
				nei := make([]edgeT, 0)
				nei = append(nei, edgeT{dest: from, value: 1})
				vertices[to] = vertexInfo{active: true, neighbors: nei}
			}
		}
	}
	fmt.Println("Vertices result")
	fmt.Println(len(vertices))
	for key, val := range vertices {
		fmt.Println("key:", key, " active:", val.active, ", neighbors:", val.neighbors)
	}
}

func initialize() {
	stepcount = 0

	newMasterMsg := <-masterChan
	fmt.Println("Entered initialize()")
	datasetFilename = newMasterMsg.GetDatasetFilename()

	updateWorkerIDs()
	dataset = sdfs.GetGraphInput(datasetFilename)
	updateVertex()
}

/* worker related function */
func computeAllVertex() {
	for vertexId, info := range vertices {
		info.Compute()
	}
}

/* master related function */
func listenMaster() {
	ln, err := net.Listen("tcp", masterworkerPort)
	if err != nil {
		fmt.Println("cannot listen on port", masterworkerPort, err.Error())
		return
	}
	defer ln.Close()

	for {
		var buf bytes.Buffer

		conn, err := ln.Accept()
		func() {
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
			fmt.Println(masterMsg)
			if masterMsg.GetCommand() == START {
				go initialize()
			}

			masterChan <- masterMsg
			if masterMsg.GetSource() != masterID {
				masterID = masterMsg.GetSource()
			}

		}()
	}
}

func main() {
	//TODO: get myid from hostname
	masterChan = make(chan ssproto.Superstep)
	go sdfs.Start()
	myID = util.GetIDFromHostname()
	vertices = make(map[int]vertexInfo)
	masterID = 9
	go listenMaster()
	for {
		time.Sleep(time.Second)
	}
}
