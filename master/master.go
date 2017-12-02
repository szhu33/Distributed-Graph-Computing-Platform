package main

import (
	"bytes"
	"cs425_mp4/failure-detector"
	"cs425_mp4/protocol-buffer/master-client"
	"cs425_mp4/protocol-buffer/superstep"
	"cs425_mp4/sdfs"
	"cs425_mp4/utility"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
)

const (
	masterID         = 10
	standbyID        = 9
	clientPort       = ":1234"
	masterworkerPort = ":5558"
	nodeName         = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	START            = superstep.Superstep_START
	RUN              = superstep.Superstep_RUN
	ACK              = superstep.Superstep_ACK
	VOTETOHALT       = superstep.Superstep_VOTETOHALT
	datasetName      = "input.txt"
)

var (
	systemHalt    bool // system state, need all workers vote to halt twice
	workerNum     int
	myID          int
	clientID      int
	workerInfos   map[uint32]superstep.Superstep_Command // key:vmID, value:value:ACK or vote to halt
	stepcount     int
	workerRes     chan superstep.Superstep
	workerFailure chan int
	masterFailure chan bool
	clientRequest masterclient.MasterClient
	app           string
	finalRes      []byte
	isStandBy     bool
)

/* failre handling function */
func detectFailure() {
	for {
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

func standByUp() {

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

		conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, standbyID, clientPort))
		//conn, err := net.Dial("tcp", "localhost"+clientPort)
		if err != nil {
			fmt.Printf("error has occured! %s\n", err)
			return
		}
		defer conn.Close()
		conn.Write(pb)
	}
}

func sendClientRes() {
	msg := &masterclient.MasterClient{ClientID: uint32(clientID)}
	msg.Application = app
	msg.Result = finalRes

	pb, err := proto.Marshal(msg)
	if err != nil {
		fmt.Println("error occured!")
		return
	}

	conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, clientID, clientPort))
	//conn, err := net.Dial("tcp", "localhost"+clientPort)
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}
	defer conn.Close()
	conn.Write(pb)

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

	conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, destID, masterworkerPort))
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}
	defer conn.Close()
	conn.Write(pb)
}

func initialize() {
	stepcount = 0
	workerRes = make(chan superstep.Superstep)
	workerInfos = make(map[uint32]superstep.Superstep_Command)
	membersStatus := fd.MemberStatus()
	fmt.Println(membersStatus)
	for i := 0; i < len(membersStatus); i++ {
		if i == clientID || i == standbyID || i == masterID {
			continue
		}
		workerInfos[uint32(i)] = ACK
	}
}

func allVoteToHalt() bool {
	for _, value := range workerInfos {
		if value != VOTETOHALT {
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
		buf := make([]byte, (4 + 8 + 8))
		var pb superstep.Superstep

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

		proto.Unmarshal(buf, &pb)
		workerRes <- pb
	}
}

func startComputeGraph() {
	sendCount := 0

COMPUTE:
	for !allVoteToHalt() {
		for key := range workerInfos {
			go sendMsgToWorker(key, RUN)
			sendCount++
		}

		for sendCount != 0 {
			select {
			case res := <-workerRes:
				{
					sendCount--
					// update workerInfos
					workerInfos[res.GetSource()] = res.GetCommand()
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
	}
}

func main() {
	go sdfs.Start()
	// go detectFailure()
	myID = util.GetIDFromHostname() + 1
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
			<-masterFailure
			standByUp()
		}

		finalRes = []byte("yes")
		sendClientRes()
	}
}
