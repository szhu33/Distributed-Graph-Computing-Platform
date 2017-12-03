package main

import (
	"bytes"
	"container/heap"
	"cs425_mp4/failure-detector"
	"cs425_mp4/protocol-buffer/master-client"
	"cs425_mp4/protocol-buffer/superstep"
	"cs425_mp4/sdfs"
	"cs425_mp4/utility"
	"cs425_mp4/utility/heap"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
)

const (
	masterID         = 9
	standbyID        = 8
	standbyPort      = ":3366"
	clientPort       = ":1234"
	masterworkerPort = ":5558"
	nodeName         = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	START            = superstep.Superstep_START
	RUN              = superstep.Superstep_RUN
	ACK              = superstep.Superstep_ACK
	VOTETOHALT       = superstep.Superstep_VOTETOHALT
	datasetName      = "input.txt"
)

type workerStepState struct {
	stepNum int
	state   superstep.Superstep_Command
}

var (
	systemHalt    bool // system state, need all workers vote to halt twice
	workerNum     int
	myID          int
	clientID      int
	workerInfos   map[uint32]workerStepState
	stepcount     int
	workerRes     chan superstep.Superstep
	workerFailure chan int
	masterFailure chan bool
	clientRequest masterclient.MasterClient
	app           string
	finalRes      *maxheap.VertexHeap
	isStandBy     bool
	standbyCount  int
)

/* failre handling function */
func detectFailure() {
	for {
		if !fd.GetIsInitialized() {
			time.Sleep(time.Microsecond)
			continue
		}
		memberStatus := fd.MemberStatus()
		for i := 0; i < len(memberStatus); i++ {
			if !memberStatus[i] {
				if i == clientID {
					continue
				} else if i == masterID {
					isStandBy = false
					masterFailure <- true
				} else {
					workerFailure <- i
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func sendStandbyStepcount() {
	msg := &superstep.Superstep{Source: uint32(myID)}
	msg.Stepcount = uint64(stepcount)

	pb, err := proto.Marshal(msg)
	if err != nil {
		fmt.Println("error occured!")
		return
	}

	conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, standbyID+1, standbyPort))
	if err != nil {
		fmt.Printf("sendStandbyStepcount: error has occured! %s\n", err)
		return
	}
	defer conn.Close()
	conn.Write(pb)
}

func standbyReivStepcount() {
	ln, err := net.Listen("tcp", standbyPort)
	if err != nil {
		fmt.Println("cannot listen on port")
		return
	}
	defer ln.Close()
	fmt.Printf("listening on port %s\n", clientPort)
	buf := make([]byte, 256)
	for {

		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("error occured!")
			return
		}
		defer conn.Close()

		_, err = conn.Read(buf)
		if err != nil {
			fmt.Println("error occured!")
			return
		}

		var newStepcount superstep.Superstep
		proto.Unmarshal(buf, &newStepcount)
		stepcount = int(newStepcount.GetStepcount())
		standbyCount = workerNum
		fmt.Printf("new meassge form master, stepcount: %d\n", stepcount)

	}

}

func standbyWait() {
	<-masterFailure
	isStandBy = false
	standbyUp()
}
func standbyUp() {
	for standbyCount > 0 {
		res := <-workerRes
		{
			standbyCount--
			// update workerInfos
			if res.GetStepcount() == uint64(stepcount) {
				workerInfos[res.GetSource()] = workerStepState{stepNum: int(res.GetStepcount()), state: res.GetCommand()}
			}
		}
	}
	sendCount := 0
	for !allVoteToHalt() {
		// send worker to run next step
		for key := range workerInfos {
			cmd := RUN
			if stepcount == 0 {
				cmd = START
			}
			go sendMsgToWorker(key, cmd)
			sendCount++
		}

		for sendCount != 0 {
			res := <-workerRes
			sendCount--
			// update workerInfos
			if res.GetStepcount() == uint64(stepcount) {
				workerInfos[res.GetSource()] = workerStepState{stepNum: int(res.GetStepcount()), state: res.GetCommand()}
			}
		}
		stepcount++
	}
}

// upload dataset into sdfs TODO: implement this function
func uploadDataToSDFS() bool {
	fmt.Println("start uploading to sdfs")
	return sdfs.PutSendAndWaitACK(clientRequest.GetDataset(), datasetName, time.Now())
}

/* client related function */
func listenClient() {
	ln, err := net.Listen("tcp", clientPort)
	if err != nil {
		fmt.Println("cannot listen on port")
		return
	}
	defer ln.Close()
	fmt.Printf("listening on port %s\n", clientPort)
	var buf bytes.Buffer

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("error occured!")
		return
	}
	defer conn.Close()

	_, err = io.Copy(&buf, conn)
	if err != nil {
		fmt.Println("error occured!")
		return
	}

	proto.Unmarshal(buf.Bytes(), &clientRequest)
	clientID = int(clientRequest.GetClientID())
	fmt.Printf("unmarshal new meassge, client id: %d\n", clientRequest.GetClientID())

	// if not standby, need to send request to clientID to standbyMaster through clientPort
	if !isStandBy {
		masterToStandby := &masterclient.MasterClient{}
		masterToStandby.ClientID = clientRequest.GetClientID()
		masterToStandby.Application = clientRequest.GetApplication()

		pb, err := proto.Marshal(masterToStandby)
		if err != nil {
			fmt.Println("error occured!")
			return
		}

		conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, standbyID+1, clientPort))
		//conn, err := net.Dial("tcp", "localhost"+clientPort)
		if err != nil {
			fmt.Printf("listenClient: error has occured! %s\n", err)
			return
		}
		defer conn.Close()
		conn.Write(pb)
	}
}

func sendClientRes() {
	msg := &masterclient.MasterClient{ClientID: uint32(clientID)}
	msg.Application = app
	// msg.Result = finalRes

	pb, err := proto.Marshal(msg)
	if err != nil {
		fmt.Println("error occured!")
		return
	}

	conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, clientID+1, clientPort))
	//conn, err := net.Dial("tcp", "localhost"+clientPort)
	if err != nil {
		fmt.Printf("sendClientRes: error has occured! %s\n", err)
		return
	}
	defer conn.Close()
	conn.Write(pb)
	getAllResults()
}

/* worker related function */
// send msg to node
func sendMsgToWorker(destID uint32, command superstep.Superstep_Command) {
	msg := &superstep.Superstep{Source: uint32(myID)}
	msg.Command = command
	msg.Stepcount = uint64(stepcount)
	if command == START {
		msg.DatasetFilename = datasetName
	}
	pb, err := proto.Marshal(msg)
	if err != nil {
		fmt.Println("error occured!")
		return
	}

	conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, destID+1, masterworkerPort))
	if err != nil {
		fmt.Printf("sendMsgToWorker: error has occured! %s\n", err)
		return
	}
	defer conn.Close()
	conn.Write(pb)
}

func initialize() {
	stepcount = 0
	workerRes = make(chan superstep.Superstep)
	workerInfos = make(map[uint32]workerStepState)
	membersStatus := fd.MemberStatus()
	for i := 0; i < len(membersStatus); i++ {
		if i == clientID || i == standbyID || i == masterID {
			continue
		}
		workerInfos[uint32(i)] = workerStepState{stepNum: stepcount, state: ACK}
	}
}

func getAllResults() {
	// send all workers ACK to stop computing

	// initialize a heap
	finalRes = &maxheap.VertexHeap{}
	heap.Init(finalRes)

	sendCount := 0
	for key := range workerInfos {
		cmd := ACK
		go sendMsgToWorker(key, cmd)
		sendCount++
	}
	for sendCount != 0 {
		res := <-workerRes
		sendCount--
		workerResult := res.GetVertices()
		for key := range workerResult {
			finalRes.Push(workerResult[key])
		}
		// finalRes = append(finalRes, res.GetVertices()...)
		fmt.Printf("received a result, sendcount-- now is %d\n", sendCount)
	}
	for finalRes.Len() > 0 {
		elem := finalRes.Pop().(*superstep.Vertex)
		fmt.Println(*elem)
	}
}
func allVoteToHalt() bool {
	for _, value := range workerInfos {
		if value.state != VOTETOHALT {
			return false
		}
	}
	return true
}

func listenWorker() {
	ln, err := net.Listen("tcp", masterworkerPort)
	if err != nil {
		fmt.Println("cannot listen on port")
		return
	}
	defer ln.Close()

	for {
		var buf bytes.Buffer

		var pb superstep.Superstep

		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("error occured!")
			return
		}
		defer conn.Close()
		_, err = io.Copy(&buf, conn)
		if err != nil {
			fmt.Println("error occured!")
			return
		}

		proto.Unmarshal(buf.Bytes(), &pb)
		if !isStandBy {
			fmt.Printf("received ACK form worker: %d\n", pb.GetSource())
			workerRes <- pb
		} else {
			if int(pb.GetStepcount()) == stepcount {
				fmt.Printf("received ACK form worker: %d\n", pb.GetSource())
				standbyCount--
			}
		}
	}
}

func startComputeGraph() {
	sendCount := 0

COMPUTE:
	for !allVoteToHalt() {
		// send standby master the stepcount
		go sendStandbyStepcount()
		// send worker to run next step
		time.Sleep(50 * time.Millisecond)
		for key := range workerInfos {
			cmd := RUN
			if stepcount == 0 {
				cmd = START
			}
			go sendMsgToWorker(key, cmd)
			sendCount++
		}

		for sendCount != 0 {
			select {
			case res := <-workerRes:
				{
					sendCount--
					// update workerInfos
					fmt.Printf("sendcount-- now is %d\n", sendCount)
					if res.GetStepcount() == uint64(stepcount) {
						workerInfos[res.GetSource()] = workerStepState{stepNum: int(res.GetStepcount()), state: res.GetCommand()}
					}
				}
			case id := <-workerFailure:
				{
					if id != standbyID {
						fmt.Println("a worker failed, restart right now")
						// restart
						initialize()
						for key := range workerInfos {
							go sendMsgToWorker(key, START)
						}
						time.Sleep(1)
						continue COMPUTE
					} else {
						fmt.Println("standby master failed, continue computing")
					}
				}
			}
		}
		stepcount++
		fmt.Println("stepcount ++, now is", stepcount)
	}
	getAllResults()
}

func main() {
	go sdfs.Start()
	go detectFailure()
	myID = util.GetIDFromHostname()
	if myID != masterID {
		isStandBy = true
	}
	for {
		listenClient()
		app = clientRequest.GetApplication()
		go listenWorker()
		initialize()
		if !isStandBy {
			uploadState := uploadDataToSDFS()
			fmt.Println("upload successfully:", uploadState)
			startComputeGraph()
		} else {
			go standbyReivStepcount()
			standbyWait()
		}

		sendClientRes()
	}
}
