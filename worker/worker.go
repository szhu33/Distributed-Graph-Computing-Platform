package main

import (
	"bufio"
	"bytes"
	fd "cs425_mp4/failure-detector"
	ssproto "cs425_mp4/protocol-buffer/superstep"
	"cs425_mp4/protocol-buffer/worker-worker"
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
	HALT             = ssproto.Superstep_VOTETOHALT
	VOTETOHALT       = ssproto.Superstep_VOTETOHALT
	localInputName   = "localFile.txt"
)

type vertexInfo struct {
	neighbors []edgeT
	VertexPageRank
}

type edgeT struct {
	dest  int
	value float64
}

var (
	vertices        map[int]vertexInfo
	stepcount       uint64
	myID            int
	masterID        uint32
	initChan        chan ssproto.Superstep
	computeChan     chan ssproto.Superstep
	masterMsg       ssproto.Superstep
	workerIDs       [totalNodes]int //should range from 0-9
	datasetFilename string
	dataset         []byte
	idToVM          map[int]int
	restartFlag     = false
	active          map[int]bool
	msgQueue        map[int][]*workerpb.Worker
	nextMsgQueue    map[int][]*workerpb.Worker
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
		if err != nil {
			fmt.Println("Error when parsing Int!", err.Error())
		}
		from := int(from1)
		dummyFromInt := from
		fromVM := int(util.HashToVMIdx(string(dummyFromInt)))
		for !isInWorkerIDs(fromVM) {
			dummyFromInt++
			fromVM = int(util.HashToVMIdx(string(dummyFromInt)))
		}
		to1, err := strconv.ParseInt(words[1], 10, 32)
		to := int(to1)
		dummyToInt := to
		toVM := int(util.HashToVMIdx(string(dummyToInt)))
		for !isInWorkerIDs(toVM) {
			dummyToInt++
			toVM = int(util.HashToVMIdx(string(dummyToInt)))
		}
		// fmt.Printf("fromvertex:%d, tovertex:%d, fromVM:%d, toVm:%d\n", from, to, fromVM, toVm)
		idToVM[from] = fromVM
		idToVM[to] = toVM

		if (fromVM != myID) && (toVM != myID) {
			continue
		}
		if fromVM == myID {
			active[from] = true
			if _, ok := vertices[from]; ok {
				tempInfo := vertices[from]
				fmt.Println("however from ! 5555555")
				tempInfo.VertexPageRank.Value = 1
				tempInfo.VertexPageRank.Id = from
				tempInfo.neighbors = append(tempInfo.neighbors, edgeT{dest: to, value: 1})
				vertices[from] = tempInfo
			} else {
				nei := make([]edgeT, 0)
				nei = append(nei, edgeT{dest: to, value: 1})
				vpr := VertexPageRank{Id: from, Value: 1}
				fmt.Println(vpr)
				vertices[from] = vertexInfo{neighbors: nei, VertexPageRank: vpr}
				msgQueue[from] = make([]*workerpb.Worker, 0)
				nextMsgQueue[from] = make([]*workerpb.Worker, 0)
			}
		}
		if toVM == myID {
			active[toVM] = true
			if _, ok := vertices[to]; ok {
				tempInfo := vertices[to]
				fmt.Println("however to 5555555")
				tempInfo.VertexPageRank.Value = 1
				tempInfo.VertexPageRank.Id = to
				tempInfo.neighbors = append(tempInfo.neighbors, edgeT{dest: from, value: 1})
				vertices[to] = tempInfo
			} else {
				nei := make([]edgeT, 0)
				nei = append(nei, edgeT{dest: from, value: 1})
				vpr := VertexPageRank{Id: to, Value: 1}
				fmt.Println(vpr)
				vertices[to] = vertexInfo{neighbors: nei, VertexPageRank: vpr}
				msgQueue[to] = make([]*workerpb.Worker, 0)
				nextMsgQueue[to] = make([]*workerpb.Worker, 0)
			}
		}
	}
	fmt.Println("Vertices result")
	fmt.Println(len(vertices))
	for key, val := range vertices {
		fmt.Println("key:", key, " active:", active[key], ", neighbors:", val.neighbors)
	}
	fmt.Println(idToVM)
}

func initialize() {
	stepcount = 0
	vertices = make(map[int]vertexInfo)
	idToVM = make(map[int]int)
	active = make(map[int]bool)
	msgQueue = make(map[int][]*workerpb.Worker)
	nextMsgQueue = make(map[int][]*workerpb.Worker)
	newMasterMsg := <-initChan
	fmt.Println("Entered initialize()")
	datasetFilename = newMasterMsg.GetDatasetFilename()

	updateWorkerIDs()
	dataset = sdfs.GetGraphInput(datasetFilename)
	updateVertex()
	computeAllVertex()
}

/* worker related function */
func computeAllVertex() {
	fmt.Println("vertices on entering computeAllvertex:", len(vertices))

	for {
		for key := range vertices {
			info := vertices[key]
			if !active[key] {
				continue
			}
			var mq = &vertexMsgQ{queue: msgQueue[key], index: 0}
			active[key] = info.Compute(mq)
			vertices[key] = info
		}
		allHalt := true
		for key := range vertices {
			if active[key] {
				allHalt = false
			}
		}
		if allHalt {
			sendToMaster(HALT)
		} else {
			sendToMaster(ACK)
		}
		nextCmd := <-computeChan
		if nextCmd.GetCommand() == START || nextCmd.GetCommand() == ACK {
			fmt.Println(nextCmd.GetCommand().String())
			returnResults()
			return
		}
		stepcount = nextCmd.GetStepcount()
		for key := range vertices {
			info := vertices[key]
			msgQueue[key] = nextMsgQueue[key]
			nextMsgQueue[key] = make([]*workerpb.Worker, 0)
			vertices[key] = info
		}
	}

}

func returnResults() {

	ret := make([]*ssproto.Vertex, 0)

	fmt.Println("After computation:")
	fmt.Println(len(vertices))
	for key, val := range vertices {
		fmt.Println("key:", key, " active:", active[key], ", neighbors:", val.neighbors, val.VertexPageRank)
	}

	for key, info := range vertices {
		newV := ssproto.Vertex{}
		newV.Value = info.VertexPageRank.GetValue()
		newV.Id = uint64(key)
		fmt.Println("key in vertices: ", key, "in VertexPageRank", newV)
		ret = append(ret, &newV)
	}
	newMsg := &ssproto.Superstep{Source: uint32(myID)}
	newMsg.Vertices = ret
	fmt.Println("my result:")
	for _, elem := range ret {
		fmt.Println(*elem)
	}
	pb, err := proto.Marshal(newMsg)
	if err != nil {
		fmt.Println("Unmarshall: error occured!", err.Error())
		return
	}
	conn, err := net.Dial("tcp", util.HostnameStr(int(masterID+1), masterworkerPort))
	if err != nil {
		fmt.Println("Send result: Dial to master failed!", err.Error())
	}
	defer conn.Close()
	conn.Write(pb)
}

func sendToMaster(cmd ssproto.Superstep_Command) {
	newMsg := &ssproto.Superstep{Source: uint32(myID), Command: cmd, Stepcount: stepcount}
	pb, err := proto.Marshal(newMsg)
	if err != nil {
		fmt.Println("Error when marshal halt message.", err.Error())
	}

	conn, err := net.Dial("tcp", util.HostnameStr(int(masterID+1), masterworkerPort))
	if err != nil {
		fmt.Println("Dial to master failed!", err.Error())
	}
	defer conn.Close()
	conn.Write(pb)
}

func sendToWorker(msgpb *workerpb.Worker) {
	toVertexID := int(msgpb.GetToVertex())
	dest := idToVM[toVertexID]
	// if dest == myID {
	// 	// Insert to local queue
	// 	temp := vertices[toVertexID]
	// 	temp.nextMsgQueue = append(temp.nextMsgQueue, msgpb)
	// 	vertices[toVertexID] = temp
	// } else {
	// Send to other worker
	pb, err := proto.Marshal(msgpb)
	if err != nil {
		fmt.Println("Error when marshal worker-worker message.", err.Error())
	}
	conn, err := net.Dial("tcp", util.HostnameStr(dest+1, workerPort))
	if err != nil {
		fmt.Println("Dial to worker failed!", err.Error())
	}
	defer conn.Close()
	conn.Write(pb)
	// }
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
			if masterMsg.GetSource() != masterID {
				masterID = masterMsg.GetSource()
			}
			if masterMsg.GetCommand() == START {
				if restartFlag {
					computeChan <- masterMsg
				}
				restartFlag = true
				go initialize()
				initChan <- masterMsg
			} else {
				computeChan <- masterMsg
			}

		}()
	}
}

/* master related function */
func listenWorker() {
	ln, err := net.Listen("tcp", workerPort)
	if err != nil {
		fmt.Println("cannot listen on port", workerPort, err.Error())
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

			newWorkerMsg := &workerpb.Worker{}
			err := proto.Unmarshal(buf.Bytes(), newWorkerMsg)
			if err != nil {
				fmt.Println("listenWorker: Error unmarshall.", err.Error())
			}
			fmt.Println(newWorkerMsg)
			toVertexID := int(newWorkerMsg.GetToVertex())
			tempQ := nextMsgQueue[toVertexID]
			tempQ = append(tempQ, newWorkerMsg)
			nextMsgQueue[toVertexID] = tempQ
			active[toVertexID] = true
		}()
	}
}

func main() {
	//TODO: get myid from hostname
	initChan = make(chan ssproto.Superstep)
	computeChan = make(chan ssproto.Superstep)
	go sdfs.Start()
	myID = util.GetIDFromHostname()
	masterID = 9
	go listenMaster()
	go listenWorker()
	for {
		time.Sleep(time.Second)
	}
}

// NumVertices returns the number of vertices in the graph
func NumVertices() int {
	return len(idToVM)
}
