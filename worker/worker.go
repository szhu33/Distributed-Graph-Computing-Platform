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
	"sync"
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
	APP1_PR          = "PageRank"
	APP2_SSSP        = "SSSP"
)

type vertexInfo struct {
	VertexPageRank
	VertexSSSP
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
	initChan        chan *ssproto.Superstep
	computeChan     chan *ssproto.Superstep
	workerMsgChan   chan bool
	masterMsg       ssproto.Superstep
	workerIDs       [totalNodes]int //should range from 0-9
	datasetFilename string
	dataset         []byte
	idToVM          map[int]int
	restartFlag     = false
	active          map[int]bool
	msgQueue        map[int][]*workerpb.Worker
	nextMsgQueue    map[int][]*workerpb.Worker
	neighborMap     map[int][]edgeT
	appName         string
	SSSP_source     int

	msgQMutex   = &sync.Mutex{}
	activeMutex = &sync.Mutex{}

	combinerMsg map[int]*workerpb.WorkerTotal
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

	lines := 0
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
		lines += 1
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
				tempInfo.VertexPageRank.Value = 1
				tempInfo.VertexPageRank.Id = from
				vertices[from] = tempInfo
				tempN := neighborMap[from]
				tempN = append(tempN, edgeT{dest: to, value: 1})
				neighborMap[from] = tempN
			} else {
				nei := make([]edgeT, 0)
				nei = append(nei, edgeT{dest: to, value: 1})
				vpr := VertexPageRank{Id: from, Value: 1}
				vertices[from] = vertexInfo{VertexPageRank: vpr}
				neighborMap[from] = nei
				msgQueue[from] = make([]*workerpb.Worker, 0)
				nextMsgQueue[from] = make([]*workerpb.Worker, 0)
			}
		}
		if toVM == myID {
			active[to] = true
			if _, ok := vertices[to]; ok {
				tempInfo := vertices[to]
				tempInfo.VertexPageRank.Value = 1
				tempInfo.VertexPageRank.Id = to
				tempN := neighborMap[to]
				tempN = append(tempN, edgeT{dest: from, value: 1})
				neighborMap[to] = tempN
				vertices[to] = tempInfo
			} else {
				nei := make([]edgeT, 0)
				nei = append(nei, edgeT{dest: from, value: 1})
				vpr := VertexPageRank{Id: to, Value: 1}
				vertices[to] = vertexInfo{VertexPageRank: vpr}
				neighborMap[to] = nei
				msgQueue[to] = make([]*workerpb.Worker, 0)
				nextMsgQueue[to] = make([]*workerpb.Worker, 0)
			}
		}
	}
	fmt.Println("# Lines:", lines)
	fmt.Println("# Vertices:", NumVertices())
	// fmt.Println("Vertices len:", len(vertices))
	// fmt.Println()
	// for key, val := range vertices {
	// 	fmt.Println("key:", key, " active:", active[key], ", neighbors:", neighborMap[key], val)
	// }
	// fmt.Println(idToVM)
	workerMsgChan <- true
}

func initialize() {
	stepcount = 0
	vertices = make(map[int]vertexInfo)
	idToVM = make(map[int]int)
	active = make(map[int]bool)
	msgQueue = make(map[int][]*workerpb.Worker)
	nextMsgQueue = make(map[int][]*workerpb.Worker)
	neighborMap = make(map[int][]edgeT)
	newMasterMsg := <-initChan
	fmt.Println("Entered initialize()")
	datasetFilename = newMasterMsg.GetDatasetFilename()
	appName = newMasterMsg.GetApplication()
	fmt.Println("Application:", appName)
	updateWorkerIDs()
	dataset = sdfs.GetGraphInput(datasetFilename)
	updateVertex()
	computeAllVertex()
}

/* worker related function */
func computeAllVertex() {
	fmt.Println("vertices on entering computeAllvertex:", len(vertices))

	for {
		combinerMsg = make(map[int]*workerpb.WorkerTotal)
		for key := range vertices {
			info := vertices[key]
			if !active[key] {
				continue
			}
			var mq = &vertexMsgQ{queue: msgQueue[key], index: 0}
			activeMutex.Lock()
			if appName == APP1_PR {
				active[key] = info.VertexPageRank.Compute(mq)
			} else if appName == APP2_SSSP {
				active[key] = info.VertexSSSP.Compute(mq)
			}
			activeMutex.Unlock()
			vertices[key] = info
		}

		for key, value := range combinerMsg {
			go func(key int, value *workerpb.WorkerTotal) {
				pb, err := proto.Marshal(value)
				if err != nil {
					fmt.Println("Error when marshal worker-worker message.", err.Error())
					return
				}
				conn, err := net.Dial("tcp", util.HostnameStr(key+1, workerPort))
				if err != nil {
					fmt.Println("Dial to worker failed!", err.Error())
					return
				}
				defer conn.Close()
				conn.Write(pb)
			}(key, value)
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
		fmt.Println("Superstep:", stepcount)
		nextCmd := <-computeChan
		if nextCmd.GetCommand() == ACK {
			fmt.Println(nextCmd.GetCommand().String())
			returnResults()
			return
		}
		if nextCmd.GetCommand() == START {
			fmt.Println(nextCmd.GetCommand().String())
			return
		}
		time.Sleep(50 * time.Millisecond)
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
	//
	// fmt.Println("After computation:")
	// fmt.Println(len(vertices))
	// for key, val := range vertices {
	// 	fmt.Println("key:", key, " active:", active[key], ", neighbors:", neighborMap[key], val.VertexPageRank)
	// }

	for key, info := range vertices {
		newV := ssproto.Vertex{}
		if appName == APP1_PR {
			newV.Value = info.VertexPageRank.GetValue()
		} else if appName == APP2_SSSP {
			newV.Value = info.VertexSSSP.GetValue()
		}
		newV.Id = uint64(key)
		// fmt.Println("key in vertices: ", key, "in VertexPageRank", newV)
		ret = append(ret, &newV)
	}
	newMsg := &ssproto.Superstep{Source: uint32(myID)}
	newMsg.Vertices = ret
	// fmt.Println("my result:")
	// for _, elem := range ret {
	// 	fmt.Println(*elem)
	// }
	pb, err := proto.Marshal(newMsg)
	if err != nil {
		fmt.Println("Unmarshall: error occured!", err.Error())
		return
	}
	conn, err := net.Dial("tcp", util.HostnameStr(int(masterID+1), masterworkerPort))
	if err != nil {
		fmt.Println("Send result: Dial to master failed!", err.Error())
		return
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
	if dest == myID {
		// Insert to local queue
		msgQMutex.Lock()
		temp := nextMsgQueue[toVertexID]
		temp = append(temp, msgpb)
		nextMsgQueue[toVertexID] = temp
		msgQMutex.Unlock()
	} else {
		// Send to other worker
		if _, ok := combinerMsg[dest]; ok {
			temp := combinerMsg[dest]
			temp.Vertices = append(temp.Vertices, msgpb)
			combinerMsg[dest] = temp
		} else {
			newWorkTotal := workerpb.WorkerTotal{}
			newVertices := make([]*workerpb.Worker, 0)
			newVertices = append(newVertices, msgpb)
			newWorkTotal.Vertices = newVertices
			combinerMsg[dest] = &newWorkTotal
		}
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
		if err != nil {
			fmt.Println("error occured!", err.Error())
			return
		}
		go func(conn net.Conn) {
			defer conn.Close()

			_, err = io.Copy(&buf, conn)
			if err != nil {
				fmt.Println("error occured!", err.Error())
				return
			}

			newMsg := &ssproto.Superstep{}

			proto.Unmarshal(buf.Bytes(), newMsg)
			// fmt.Println(masterMsg)
			if newMsg.GetSource() != masterID {
				masterID = newMsg.GetSource()
			}
			if newMsg.GetCommand() == START {
				if restartFlag {
					computeChan <- newMsg
				}
				restartFlag = true
				go initialize()
				initChan <- newMsg
			} else {
				computeChan <- newMsg
			}
		}(conn)
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

	<-workerMsgChan

	for {
		var buf bytes.Buffer

		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("error occured!", err.Error())
			return
		}
		go func(conn net.Conn) {
			defer conn.Close()

			_, err = io.Copy(&buf, conn)
			if err != nil {
				fmt.Println("error occured!", err.Error())
				return
			}

			newWorkerMsg := &workerpb.WorkerTotal{}
			err1 := proto.Unmarshal(buf.Bytes(), newWorkerMsg)
			if err1 != nil {
				fmt.Println("listenWorker: Error unmarshall.", err.Error())
				return
			}
			// fmt.Println(newWorkerMsg)
			for _, value := range newWorkerMsg.Vertices {
				toVertexID := int(value.GetToVertex())
				msgQMutex.Lock()
				tempQ := nextMsgQueue[toVertexID]
				tempQ = append(tempQ, value)
				nextMsgQueue[toVertexID] = tempQ
				msgQMutex.Unlock()
				activeMutex.Lock()
				active[toVertexID] = true
				activeMutex.Unlock()
			}
		}(conn)
	}
}

func main() {
	//TODO: get myid from hostname
	initChan = make(chan *ssproto.Superstep)
	computeChan = make(chan *ssproto.Superstep)
	workerMsgChan = make(chan bool)
	go sdfs.Start()
	myID = util.GetIDFromHostname()
	masterID = 9
	go listenWorker()
	listenMaster()
}

// NumVertices returns the number of vertices in the graph
func NumVertices() int {
	return len(idToVM)
}
