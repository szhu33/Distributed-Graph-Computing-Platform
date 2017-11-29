package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"cs425_mp4/protocol-buffer/superstep"
	"cs425_mp4/protocol-buffer/master-client"
)

const (
	clientPort = "1234"
	START_BLAH = superstep.Superstep_START
)

type (
	test = masterclient.MasterClient
)

var (
  workerNum := 7
  standbyMasterID := 9
)


// seng msg to standby master



// upload dataset into sdfs
func uploadDataToSDFS() bool {

}

// start, wait ack or vote


func main() {
	myservice := new(service.RpcService)
	rpc.Register(myservice)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", clientPort)
	if e != nil {
		fmt.Println("can not listen on port!")
		return
	}
	go http.Serve(l, nil)
}
