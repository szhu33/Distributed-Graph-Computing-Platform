package sdfs

import (
	"cs425_mp4/protocol-buffer/file-transfer"
	util "cs425_mp4/utility"
	"fmt"
	"sort"
	"time"

	"github.com/golang/protobuf/ptypes"
)

// PutSendAndWaitACK put command
// graph computing master put
func PutSendAndWaitACK(buf []byte, sdfsFilename string, timestamp time.Time) bool {
	fmt.Println("enter mp3 function")
	// generate msg, hash, build tcp connection and write
	myMsg := &fileTransfer.FileTransfer{} //membership
	myMsg.Source = uint32(myID)
	myMsg.SdfsFilename = sdfsFilename
	myMsg.Command = PUT
	myMsg.File = buf
	msgTime, err := ptypes.TimestampProto(timestamp)
	if err != nil {
		fmt.Println("Error parsing time.", err.Error())
		return false
	}
	myMsg.Timestamp = msgTime

	targetIdx := util.HashToVMIdx(sdfsFilename)
	for i := 0; i < replicaNum; i++ {
		for !sMembershipList[targetIdx] {
			targetIdx++
			targetIdx = uint32(util.ModLength(int(targetIdx), listLength))
		}
		fmt.Printf("targetIdx: %d\n", targetIdx+1)
		err := util.SendWithMarshal(filePort, int(targetIdx), myMsg)
		if err != nil {
			continue
		}
		targetIdx++
		targetIdx = uint32(util.ModLength(int(targetIdx), listLength))
	}

	// wait for quorum ack
	ackNum := 0
	timer := time.NewTimer(10 * time.Second)

	for ackNum < quorum {
		select {
		case <-timer.C:
			fmt.Println("Did not get enough ACKs before timeout.")
			return false
		case newACK := <-ackPutChan:
			if newACK.GetSdfsFilename() == sdfsFilename {
				ackNum++
			}
		}
	}
	return true
}

// GetGraphInput is get file in byte format from sdfs
func GetGraphInput(sdfsFilename string) []byte {
	readTime = time.Now()
	var targetIdxs []uint32
	targetIdxs = askMasterTargets(sdfsFilename, GET)
	if len(targetIdxs) != 3 {
		fmt.Println("Failed to get the file because not enough replicas for a file read (or invalid sdfs filename). Please try again.")
		return []byte{}
	}

	// send query
	for i := 0; i < replicaNum; i++ {
		myMsg := &fileTransfer.FileTransfer{}
		myMsg.Source = uint32(myID)
		myMsg.SdfsFilename = sdfsFilename
		myMsg.Command = GET
		util.SendWithMarshal(queryPort, int(targetIdxs[i]), myMsg)
	}
	// wait response
	myAllResponses := make(allResponses, 0, replicaNum)
	timer := time.NewTimer(time.Second * 30) // 30s timeout if didn't get enough response
GRAPH_GET_WAIT:
	for responseNum := 0; responseNum < replicaNum; {
		select {
		case <-timer.C:
			fmt.Println("Timeout! waited for 30secs and didn't get all replicas")
			break GRAPH_GET_WAIT
		case response := <-fileGetChan:
			myAllResponses = append(myAllResponses, *response)
			responseNum++
		}
	}
	sort.Sort(myAllResponses)

	// store the lastest one
	lastest := myAllResponses[0]

	b := lastest.GetFile()

	fmt.Println("successfully get the sdfs and store it as local. Initializing read repair now!")
	myLog.Printf("Successfully get the sdfs file %s\n", sdfsFilename)
	// initiate read-repair
	tlastest, _ := ptypes.Timestamp(lastest.GetTimestamp())
	for i := 1; i < len(myAllResponses); i++ {
		t, _ := ptypes.Timestamp(myAllResponses[i].GetTimestamp())
		if t.Before(tlastest) {
			//TODO: implement real read repair
			fmt.Println("send read repair query!")
		}
	}
	fmt.Println("Read time:", time.Since(readTime))
	fmt.Println("read repairment done!")
	return b
}
