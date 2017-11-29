package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"

	"cs425_mp4/protocol-buffer/master-client"

	"github.com/golang/protobuf/proto"
)

const (
	clientPort = ":1234"
	masterID   = 10
	nodeName   = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	APP1       = "PageRank"
	APP2       = "Another"
)

var (
	myID            int
	clientID        int
	standbyMasterID int
	output          masterclient.MasterClient
)

// TODO:change to util!
func getIDFromHostname() int {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	fmt.Println("hostname:", hostname)
	list := strings.SplitN(hostname, ".", 2)
	if len(list) > 0 {
		tempStr := list[0]
		id, err := strconv.Atoi(tempStr[len(tempStr)-2:])
		if err != nil {
			// If not in the format of "fa17-cs425-g28-%02d.cs.illinois.edu"
			// just return 0 (to allow running in local developement)
			return 0
		}
		return id - 1
	}
	panic("No valid hostname!")
}

func main() {
	myID = getIDFromHostname()
	var app, data string

	for {
		// handle input
		fmt.Println("Please enter command like: <Application> <Dataset filename>\nApplication includes PageRank and Another\n")
		fmt.Scanln(&app, &data)

		if app != APP1 && app != APP2 {
			fmt.Println("Invalid command, please enter correct command\n")
			continue
		}

		dataset, err := ioutil.ReadFile(data)
		if err != nil {
			fmt.Println("unable to open the file, please enter correct command\n")
			continue
		}

		// send msg to master
		msg := &masterclient.MasterClient{ClientID: uint32(myID)}
		msg.Application = app
		msg.Dataset = dataset

		pb, err := proto.Marshal(msg)
		if err != nil {
			fmt.Println("error occured!")
			return
		}

		conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, masterID, clientPort))
		//conn, err := net.Dial("tcp", "localhost"+masterPort)
		if err != nil {
			fmt.Printf("error has occured! %s\n", err)
			return
		}
		defer conn.Close()
		conn.Write(pb)

		// listen on response
		fmt.Println("Command and dataset have been sent to master, waiting for response!")
		ln, err := net.Listen("tcp", clientPort)
		if err != nil {
			fmt.Printf("error has occured! %s\n", err)
			return
		}
		connln, err := ln.Accept()
		if err != nil {
			fmt.Printf("error has occured! %s\n", err)
			return
		}
		defer connln.Close()
		buf := make([]byte, 1024*1024)
		_, err = connln.Read(buf)
		if err != nil {
			fmt.Printf("error has occured! %s\n", err)
			return
		}

		proto.Unmarshal(buf, &output)

		fmt.Println("get output!")
		fmt.Println(string(output.GetResult()))
	}
}
