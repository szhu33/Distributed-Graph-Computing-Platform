package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
  "cs425_mp4/rpc_service"
)

const (
	clientPort = "1234"
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
