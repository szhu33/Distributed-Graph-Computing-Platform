package main

import (
	"bytes"
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
	clientPort       = ":1234"
	masterworkerPort = ":5558"
	nodeName         = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	START            = superstep.Superstep_START
	RUN              = superstep.Superstep_RUN
	ACK              = superstep.Superstep_ACK
	VOTETOHALT       = superstep.Superstep_VOTETOHALT
	datasetName      = "input.txt"
)

type (
	test = masterclient.MasterClient
)

var (
	systemHalt      bool // system state, need all workers vote to halt twice
	workerNum       int
	myID            int
	clientID        int
	standbyMasterID int
	workerInfos     map[uint32]superstep.Superstep_Command // key:vmID, value:state
	stepcount       int
	workerRes       chan superstep.Superstep
	failure         chan int
	clientRequest   masterclient.MasterClient
	app             string
	finalRes        []byte
)

/* failre handling function */
// seng msg to standby master
// update standbyMasterID and workerIDs by failure detector

// upload dataset into sdfs TODO: implement this function
func uploadDataToSDFS() bool {
	fmt.Println("start uploading to sdfs")
	return sdfs.PutSendAndWaitACK(clientRequest.GetDataset(), datasetName, time.Now())
}

/* client related function */
func listenClient() {
	fmt.Printf("enter listenClient\n")
	ln, err := net.Listen("tcp", clientPort)
	if err != nil {
		fmt.Println("cannot listen on port")
		return
	}
	defer ln.Close()
	fmt.Printf("listening on port %s\n", clientPort)
	var buf bytes.Buffer

	conn, err := ln.Accept()
	fmt.Println("ln accepted new msg from clent")
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
	fmt.Print("unmarshal meassge, client id: %d", clientRequest.GetClientID())
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
	//update workerInfos
}

func allVoteToHalt() bool {
	haltCount := 0
	for _, value := range workerInfos {
		if value == VOTETOHALT {
			haltCount++
		}
	}

	return haltCount == workerNum
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
	// test purpose TODO: introduce mp2
	for i := 0; i < workerNum; i++ {
		workerInfos[uint32(i)] = ACK
	}

	workerRes = make(chan superstep.Superstep)
	failure = make(chan int) // TODO: implement from mp2

	go listenWorker()
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
			case id := <-failure:
				{
					if id != standbyMasterID {
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
						sendCount--
					}
				}
			}
		}
		stepcount++
	}
}

func main() {
	go sdfs.Start()
	myID = util.GetIDFromHostname()
	for {
		listenClient()
		app = clientRequest.GetApplication()
		fmt.Println("data is below:")
		fmt.Println(string(clientRequest.GetDataset()))
		// TODO : upload dataset to sdfs
		state := uploadDataToSDFS()
		//startComputeGraph()
		fmt.Println("upload :", state)
		finalRes = []byte("yes")
		sendClientRes()
	}
}
