package sdfs

import (
	"bytes"
	fd "cs425_mp4/failure-detector"
	"cs425_mp4/protocol-buffer/file-transfer"
	util "cs425_mp4/utility"
	"cs425_mp4/utility/priorityqueue"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

const (
	nodeName   = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	filePort   = ":7773"
	ackPort    = ":3337"
	queryPort  = ":7373"
	listLength = 10
	replicaNum = 3
	quorum     = replicaNum/2 + 1
	nodeNum    = 10
	PUT        = fileTransfer.FileTransfer_PUT
	GET        = fileTransfer.FileTransfer_GET
	DELETE     = fileTransfer.FileTransfer_DELETE
	LS         = fileTransfer.FileTransfer_LS
	BACKUP     = fileTransfer.FileTransfer_BACKUP
	REPAIR     = fileTransfer.FileTransfer_REPAIR

	masterPort        = ":3737"
	listenPortPUT     = ":3773"
	listenPortTargets = ":7337"
	listenPortLS      = ":3777"
)

type (
	sdfsFileInfo struct {
		localFilename string
		timestamp     time.Time
	}
	allResponses []fileTransfer.FileTransfer

	/* used by master server */
	pq             = priorityqueue.PriorityQueue
	pqItem         = priorityqueue.Item
	masterFileInfo struct {
		LastPUT time.Time
		Nodes   pq
	}
)

var (
	myID            = util.GetIDFromHostname()
	myIPAdder       net.IP
	myAllSdfsFiles  map[string]sdfsFileInfo
	myLog           *log.Logger
	logFileName     = fmt.Sprintf("./sdfs-log-vm%02d.log", myID+1)
	masterID        = myID
	isPutting       = false
	putConflictChan = make(chan bool)
	ackPutChan      = make(chan *fileTransfer.FileTransfer, quorum)
	ackDeleteChan   = make(chan *fileTransfer.FileTransfer, replicaNum)
	fileGetChan     = make(chan *fileTransfer.FileTransfer)
	sMembershipList [nodeNum]bool
	oldSML          [nodeNum]bool

	/* Variables used by master only */
	isMaster       bool
	isMasterBackup bool
	masterTable    map[string]masterFileInfo

	reReplication = time.Time{}
	insertTime    = time.Time{}
	readTime      = time.Time{}
)

/* main */
func main() {
	// create log file
	logFile, err := os.Create(logFileName)
	defer logFile.Close()
	if err != nil {
		fmt.Println("didn't create the log file!" + logFileName)
	}
	myLog = log.New(logFile, "[ vm"+strconv.Itoa(myID+1)+" ] ", log.LstdFlags) //create new logger

	// for testing purpose! should delete and get sMembershipList by MP2
	intialize()
	handleUserInput()
}

/* put command */
// graph computing master put
func PutSendAndWaitACK(buf []byte, sdfsFilename string, timestamp time.Time) bool {
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

// file system put
func putClient(localFilename string, sdfsFilename string) {
	insertTime = time.Now()
	defer func() { isPutting = false }()
	// open local file
	file, err := os.Open(localFilename)
	defer file.Close()
	if err != nil {
		fmt.Printf("error! can not open local file : %s\n", localFilename)
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("error! Cannot read file status of %s\n", localFilename)
		return
	}
	fmt.Println("The size of the file is ", fileInfo.Size(), "bytes")

	// Load all file content into a buffer
	buf := make([]byte, fileInfo.Size())
	length, err := file.Read(buf)
	if err != nil {
		fmt.Printf("error! Cannot read file into buffer: %s. Length: %d\n", localFilename, length)
		return
	}
	// fmt.Printf("Read %d bytes into the buffer.\n", length)

	// ask master for write-write conflict
	currentTime := time.Now()
	if askMasterWriteConflict(sdfsFilename, currentTime) {
		fmt.Println("Write-write conflict time:", time.Since(currentTime))
		fmt.Println("A write has been made for this file in less than a minute ago!\n enter Y to continue write within 30s\n[Y\\[N]]? ")
		// use select to handle timeout
		timer := time.NewTimer(time.Second * 30)
	PUT_WAIT:
		for {
			select {
			case response := <-putConflictChan:
				if response {
					fmt.Println("Continue putting the file in SDFS.")
					break PUT_WAIT
				} else {
					fmt.Println("PUT rejected. Try again.")
					return
				}
			case <-timer.C:
				fmt.Println("Timeout! Last PUT command has been canceled!")
				return
			}
		}
	}
	success := PutSendAndWaitACK(buf, sdfsFilename, currentTime)
	// finish
	if success {
		fmt.Println("Insert time:", time.Since(insertTime))
		fmt.Println("Successfully put the file into SDFS.")
		myLog.Printf("Successfully put the local file %s as sdfs file %s", localFilename, sdfsFilename)
	}
}

func putServer(newFile *fileTransfer.FileTransfer) {
	//create file and store
	localFileName := "local_" + newFile.GetSdfsFilename()
	file, err := os.Create(localFileName)
	if err != nil {
		fmt.Printf("cant create local file, err:%s", err.Error())
		return
	}
	defer file.Close()
	_, err = file.Write(newFile.GetFile())
	if err != nil {
		fmt.Printf("cant write to local file, err:%s", err.Error())
		return
	}
	// store in myAllSdfsFiles
	t, _ := ptypes.Timestamp(newFile.GetTimestamp())
	myAllSdfsFiles[newFile.GetSdfsFilename()] = sdfsFileInfo{
		localFilename: localFileName,
		timestamp:     t,
	}
	// Return ACK to master and client
	myMsg := &fileTransfer.FileTransfer{} //membership
	myMsg.Source = uint32(myID)
	myMsg.SdfsFilename = newFile.GetSdfsFilename()
	myMsg.Timestamp = newFile.GetTimestamp()
	myMsg.Command = PUT
	myMsg.Ack = true

	var wg sync.WaitGroup
	srcID := int(newFile.GetSource())

	wg.Add(1)
	go func() {
		util.SendWithMarshal(ackPort, srcID, myMsg)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		util.SendWithMarshal(masterPort, masterID, myMsg)
		wg.Done()
	}()

	wg.Wait()
	myLog.Printf("Successfully put the sdfs file %s in this machine", newFile.GetSdfsFilename())
}

func askMasterWriteConflict(sdfsFilename string, ts time.Time) bool {
	myMsg := &fileTransfer.FileTransfer{}
	myMsg.Source = uint32(myID)
	myMsg.SdfsFilename = sdfsFilename
	myMsg.Command = PUT
	msgTime, err := ptypes.TimestampProto(ts)
	if err != nil {
		return false
	}
	myMsg.Timestamp = msgTime

	util.SendWithMarshal(masterPort, masterID, myMsg)

	/* Start listening on listenPortPUT to wait for master reply */
	tcpAddr, err := net.ResolveTCPAddr("tcp", listenPortPUT)
	if err != nil {
		println("ResolveTCPAddr failed:", err.Error())
		return false
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Printf("Error! Cannot listen listenPortPUT on client (id %d): %s", myID+1, err.Error())
		return false
	}
	defer ln.Close()
	ln.SetDeadline(time.Now().Add(5 * time.Second))

	conn, err := ln.Accept()
	if err != nil {
		fmt.Printf("Error! cannot accept when listening:%s", err.Error())
		return false
	}
	replyMsg, err := util.RecvWithUnmarshal(conn)
	if err != nil {
		return false
	}
	if replyMsg.GetSdfsFilename() == sdfsFilename {
		return replyMsg.GetWriteConflict()
	}
	return false
}

/* get command */
func getClient(sdfsFilename string, localFilename string) {
	readTime = time.Now()
	var targetIdxs []uint32
	targetIdxs = askMasterTargets(sdfsFilename, GET)
	if len(targetIdxs) != 3 {
		fmt.Println("Failed to get the file because not enough replicas for a file read (or invalid sdfs filename). Please try again.")
		return
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
GET_WAIT:
	for responseNum := 0; responseNum < replicaNum; {
		select {
		case <-timer.C:
			fmt.Println("Timeout! waited for 5secs and didn't get all replicas")
			break GET_WAIT
		case response := <-fileGetChan:
			myAllResponses = append(myAllResponses, *response)
			responseNum++
		}
	}
	sort.Sort(myAllResponses)

	// store the lastest one
	lastest := myAllResponses[0]
	localFile, err := os.Create(localFilename)
	if err != nil {
		fmt.Println("can not create local file: ", localFilename)
		return
	}
	defer localFile.Close()

	b := lastest.GetFile()
	n, err := localFile.Write(b)
	if err != nil {
		fmt.Printf("can not write to the local file! wrtie size is %d\n", n)
		return
	}
	fmt.Println("successfully get the sdfs and store it as local. Initializing read repair now!")
	myLog.Printf("Successfully get the sdfs file %s and store in local as %s", sdfsFilename, localFilename)
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
}

// implement interface for sorting
func (p allResponses) Len() int {
	return len(p)
}

func (p allResponses) Less(i, j int) bool {
	ti, _ := ptypes.Timestamp(p[i].Timestamp)
	tj, _ := ptypes.Timestamp(p[j].Timestamp)
	return tj.Before(ti)
}

func (p allResponses) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func getServer(newQuery *fileTransfer.FileTransfer) {
	sdfsFilename := newQuery.GetSdfsFilename()
	localFilename := myAllSdfsFiles[sdfsFilename].localFilename
	// open local file
	file, err := os.Open(localFilename)
	defer file.Close()
	if err != nil {
		fmt.Printf("error! can not open local file : %s\n", localFilename)
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("error! Cannot read file status of %s\n", localFilename)
		return
	}
	fmt.Println("The size of the file is ", fileInfo.Size(), "bytes")

	// Load all file content into a buffer
	buf := make([]byte, fileInfo.Size())
	length, err := file.Read(buf)
	if err != nil {
		fmt.Printf("error! Cannot read file into buffer: %s. Length: %d\n", localFilename, length)
		return
	}
	// fmt.Printf("Read %d bytes into the buffer.\n", length)

	// create msg and send
	myMsg := &fileTransfer.FileTransfer{} //membership
	myMsg.Source = uint32(myID)
	myMsg.SdfsFilename = sdfsFilename
	myMsg.Command = GET
	myMsg.File = buf
	t, _ := ptypes.TimestampProto(myAllSdfsFiles[newQuery.GetSdfsFilename()].timestamp)
	myMsg.Timestamp = t
	msg, err := proto.Marshal(myMsg)
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}
	conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, newQuery.GetSource()+1, filePort))
	if err != nil {
		fmt.Printf("Error! %s occured!\n, cann't dial", err.Error())
		return
	}
	defer conn.Close() // close before main ends
	conn.Write(msg)
	fmt.Println("GET server completed.")
}

/* delete command */
func deleteClient(sdfsFilename string) {
	var targetIdxs []uint32
	targetIdxs = askMasterTargets(sdfsFilename, DELETE)
	targetLength := len(targetIdxs)

	// send query
	for i := 0; i < targetLength; i++ {
		myMsg := &fileTransfer.FileTransfer{} //membership
		myMsg.Source = uint32(myID)
		myMsg.SdfsFilename = sdfsFilename
		myMsg.Command = DELETE
		util.SendWithMarshal(queryPort, int(targetIdxs[i]), myMsg)
	}
	// wait response
	timer := time.NewTimer(time.Second * 5) // 5s timeout if didn't get enough response
	ackNum := 0
DELETE_WAIT:
	for responseNum := 0; responseNum < targetLength; {
		select {
		case <-timer.C:
			fmt.Println("Timeout! waited for 5secs and didn't get all ACKs")
			break DELETE_WAIT
		case response := <-ackDeleteChan:
			if response.GetAck() {
				ackNum++
			}
			responseNum++
		}
	}
	if ackNum == targetLength {
		fmt.Println("successfully delete")
		myLog.Printf("Successfully delete the sdfs file %s", sdfsFilename)
	} else {
		fmt.Println("delete failed!")
		myLog.Printf("Unable to delete the sdfs file %s", sdfsFilename)
	}
}

func deleteServer(newQuery *fileTransfer.FileTransfer) {
	sdfdFilename := newQuery.GetSdfsFilename()
	myMsg := &fileTransfer.FileTransfer{}
	myMsg.Source = uint32(myID)
	myMsg.SdfsFilename = sdfdFilename
	myMsg.Command = DELETE
	_, ok := myAllSdfsFiles[sdfdFilename]
	if ok {
		delete(myAllSdfsFiles, sdfdFilename)
		myMsg.Ack = true
	} else {
		fmt.Println("didn't have the file!")
		myMsg.Ack = false
	}
	util.SendWithMarshal(ackPort, int(newQuery.GetSource()), myMsg)

	myMsg.Ack = true
	fmt.Println(myMsg)
	util.SendWithMarshal(masterPort, masterID, myMsg)

	fmt.Println("Successfully delete the sdfs file in this repleca.", sdfdFilename)
	myLog.Printf("Successfully delete the sdfs file %s in this repleca", sdfdFilename)
}

/* store command */
func listMySdfsFile() {
	fmt.Println("id\tsdfs filename\tlocal filename\ttimestamp\t")
	i := 1
	for k, v := range myAllSdfsFiles {
		fmt.Printf("%d\t%s\t%s\t%s\t\n", i, k, v.localFilename, v.timestamp.Format(time.UnixDate))
		i++
	}
	for i := 0; i < len(myAllSdfsFiles); i++ {
	}
}

/* ls command */
func lsClient(flag bool, filename string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", listenPortLS)
	if err != nil {
		println("ResolveTCPAddr failed:", err.Error())
		fmt.Println("LS command failed.")
		return
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Println("LS command failed.")
		fmt.Printf("Error! Cannot listen on node %d): %s\n", myID+1, err.Error())
		return
	}
	defer ln.Close()
	ln.SetDeadline(time.Now().Add(5 * time.Second))

	lsMsg := &fileTransfer.FileTransfer{}
	lsMsg.Source = uint32(myID)
	lsMsg.Command = LS

	util.SendWithMarshal(masterPort, masterID, lsMsg)

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("LS command failed.")
		fmt.Printf("Error! cannot accept when listening listenPortLS:%s\n", err.Error())
		return
	}

	lsReply, err := util.RecvWithUnmarshal(conn)
	if err != nil {
		return
	}
	r := bytes.NewReader(lsReply.GetAllMetadata())
	dec := gob.NewDecoder(r)
	var newMT map[string]masterFileInfo
	err = dec.Decode(&newMT)
	if err != nil {
		log.Fatal("decode error 1:", err)
	}
	if !flag {
		printMasterTable(newMT, masterID)
	} else {
		fmt.Printf("%s\t\t", filename)
		replicaNodes := masterReturnTargets(filename, newMT)
		for _, node := range replicaNodes {
			fmt.Printf("%d ", node+1)
		}
		fmt.Println()
	}
}

/* client ask */
func askMasterTargets(sdfsFilename string, cmdType fileTransfer.FileTransfer_Command) []uint32 {
	var targets []uint32

	tcpAddr, err := net.ResolveTCPAddr("tcp", listenPortTargets)
	if err != nil {
		println("ResolveTCPAddr failed:", err.Error())
		return targets
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Printf("Error! Cannot listen on master (id %d): %s\n", myID+1, err.Error())
		return targets
	}
	defer ln.Close()
	ln.SetDeadline(time.Now().Add(5 * time.Second))

	getRequest := &fileTransfer.FileTransfer{}
	getRequest.Command = cmdType
	getRequest.Source = uint32(myID)
	getRequest.SdfsFilename = sdfsFilename
	util.SendWithMarshal(masterPort, masterID, getRequest)

	conn, err := ln.Accept()
	if err != nil {
		fmt.Printf("Error! cannot accept when listening listenPortTargets:%s\n", err.Error())
		return targets
	}
	getReply, err := util.RecvWithUnmarshal(conn)
	if err != nil {
		return targets
	}
	for idx := 0; idx < len(getReply.GetNodes()) && idx < replicaNum; idx++ {
		targets = append(targets, getReply.GetNodes()[idx])
	}
	return targets
}

/* others */
func intialize() {
	myAllSdfsFiles = make(map[string]sdfsFileInfo) // original length 0, capacity 100
	// temp test , should get from API
	for i := 0; i < nodeNum; i++ {
		sMembershipList[i] = true
	}
	go fd.Start()
	go updateSMembershipList()
	go listenQuery()
	go listenAck()
	go listenFile()
	go initMaster()
}

func updateSMembershipList() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for _ = range ticker.C {
		oldSML = sMembershipList
		sMembershipList = fd.MemberStatus()
	}
}

func handleUserInput() {
	for {
		fmt.Println("Please enter \"put [localFileName] [sdfsFileName]\", \"get [sdfsFileName] [localFileName]\", \"delete [sdfsFileName]\", \"ls\" or \"store\":")
		var command string
		var filename1 string
		var filename2 string
		fmt.Scanln(&command, &filename1, &filename2)
		switch command {
		case "put":
			isPutting = true
			go putClient(filename1, filename2)
		case "get":
			isPutting = false
			go getClient(filename1, filename2)
		case "delete":
			isPutting = false
			go deleteClient(filename1)
		case "ls":
			isPutting = false
			go lsClient(true, filename1)
		case "lsall":
			isPutting = false
			go lsClient(false, filename1)
		case "store":
			isPutting = false
			listMySdfsFile()
		case "ls_mp2":
			fd.PrintMembershipList()
		case "neighbor":
			fd.PrintNeighborList()
		case "id":
			fd.PrintSelfID()
		case "join":
			fd.NodeJoin()
		case "leave":
			fd.NodeLeave()
		case "Y":
			if isPutting {
				putConflictChan <- true
				isPutting = false
			} else {
				fmt.Println("Incorrect input. Please try again.")
			}
		default:
			if isPutting {
				putConflictChan <- false
				isPutting = false
			} else {
				fmt.Println("Incorrect input. Please try again.")
			}
		}
	}
}

func listenAck() {
	ln, err := net.Listen("tcp", ackPort)
	if err != nil {
		fmt.Printf("Error! cannot listen on ackport:%s", err.Error())
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error! cannot accept when listening ACK:%s", err.Error())
			continue
		}
		newACK, err := util.RecvWithUnmarshal(conn)
		if err != nil {
			continue
		}
		cmd := newACK.GetCommand()
		// put ack
		if cmd == PUT {
			ackPutChan <- newACK
		} else { // delete ack
			ackDeleteChan <- newACK
		}
	}
}

func listenFile() {
	ln, err := net.Listen("tcp", filePort)
	if err != nil {
		fmt.Printf("Error! cannot listen on ackport:%s", err.Error())
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error! cannot accept when listening:%s", err.Error())
			continue
		}
		newFile, err := util.RecvWithUnmarshal(conn)
		if err != nil {
			continue
		}
		cmd := newFile.GetCommand()
		// onPutting
		if cmd == PUT {
			fmt.Println("listened new file, cmd is put")
			go putServer(newFile)
		} else { // get
			fileGetChan <- newFile
			fmt.Println("Incoming file from GET commands!")
		}
	}
}

func listenQuery() {
	ln, err := net.Listen("tcp", queryPort)
	if err != nil {
		fmt.Printf("Error! cannot listen on queryport:%s", err.Error())
		return
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error! cannot accept when listening:%s", err.Error())
			continue
		}
		newQuery, err := util.RecvWithUnmarshal(conn)
		if err != nil {
			continue
		}
		cmd := newQuery.GetCommand()
		// Get query
		if cmd == GET {
			go getServer(newQuery)
		} else if cmd == DELETE { // Delete query
			go deleteServer(newQuery)
		} else if cmd == REPAIR {
			go repairServer(newQuery)
		}
	}
}

func repairServer(newRepair *fileTransfer.FileTransfer) {
	dest := newRepair.GetDest()
	f := newRepair.GetSdfsFilename()
	localFileName := "local_" + f
	file, err := os.Open(localFileName)
	defer file.Close()
	if err != nil {
		fmt.Printf("error! can not open local file : %s\n", localFileName)
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("error! Cannot read file status of %s\n", localFileName)
		return
	}
	fmt.Println("The size of the file is ", fileInfo.Size(), "bytes")
	buf := make([]byte, fileInfo.Size())
	length, err := file.Read(buf)
	if err != nil {
		fmt.Printf("error! Cannot read file into buffer: %s. Length: %d\n", localFileName, length)
		return
	}
	// fmt.Printf("Read %d bytes into the buffer.\n", length)
	myMsg := &fileTransfer.FileTransfer{}
	myMsg.Command = PUT
	myMsg.Source = uint32(myID)
	myMsg.SdfsFilename = f
	myMsg.File = buf
	myMsg.Timestamp = newRepair.GetTimestamp()
	util.SendWithMarshal(filePort, int(dest), myMsg)
}

/* Master functions */
func initMaster() {
	masterTable = make(map[string]masterFileInfo)
	time.Sleep(2 * time.Second)
	isMaster, isMasterBackup = checkIsMaster(myID, sMembershipList, &masterID)
	go updateMasterStatus()
	for {
		if isMaster {
			masterServer()
		} else if isMasterBackup {
			backupServer()
		}
		time.Sleep(time.Millisecond)
	}
}

// Send out repair msgs
func updateMasterStatus() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for _ = range ticker.C {

		if isMaster {
			sendRepairMsg(oldSML, sMembershipList)
			n := 1
			for idx := myID - 1; idx >= 0; idx-- {
				if n > 2 {
					break
				}
				if sMembershipList[idx] {
					sendBackupMsg(masterPort, idx)
					n++
				}
			}
		}

		// Check if node itself is master or not
		// oldIsMaster := isMaster
		isMaster, isMasterBackup = checkIsMaster(myID, sMembershipList, &masterID)
		// if oldIsMaster == true && isMaster == false {
		// 	err := sendBackupMsg(masterPort, masterID)
		// 	for err != nil {
		// 		err = sendBackupMsg(masterPort, masterID)
		// 	}
		// }
	}
}

/* failure and rejoin */
func sendRepairMsg(old [nodeNum]bool, new [nodeNum]bool) {
	changedFlag := false
	for idx := 0; idx < nodeNum; idx++ {
		if old[idx] == true && new[idx] == false {
			for key, value := range masterTable {
				if value.Nodes != nil && value.Nodes.Exists(idx) {
					value.Nodes.Remove(idx)
					masterTable[key] = value
				}
			}
		}
		if old[idx] != new[idx] {
			changedFlag = true
		}
	}

	if changedFlag {
		fmt.Println("Old:", old)
		fmt.Println("New:", new)
		printMasterTable(masterTable, masterID)
		for filename, oldNodes := range masterTable {
			var newNodes [replicaNum]uint32
			targetIdx := util.HashToVMIdx(filename)

			for i := 0; i < replicaNum; i++ {
				for !sMembershipList[targetIdx] {
					targetIdx++
					if targetIdx >= nodeNum {
						targetIdx %= 10
					}
				}
				newNodes[i] = targetIdx
				targetIdx++
				targetIdx = uint32(util.ModLength(int(targetIdx), nodeNum))
			}
			// fmt.Println("Newnodes:", newNodes)
			var toRepair []int
			var best = -1
			for idx := 0; idx < replicaNum; idx++ {
				if !oldNodes.Nodes.Exists(int(newNodes[idx])) {
					toRepair = append(toRepair, int(newNodes[idx]))
				} else {
					if best == -1 {
						best = int(newNodes[idx])
					}
				}
			}
			if len(toRepair) > 0 && best != -1 {
				fmt.Println("Nodes to repair:", toRepair)
				for _, value := range toRepair {
					if reReplication.IsZero() {
						reReplication = time.Now()
					}
					repairMsg := &fileTransfer.FileTransfer{}
					repairMsg.Source = uint32(myID)
					repairMsg.Command = REPAIR
					repairMsg.SdfsFilename = filename
					repairMsg.Dest = uint32(value)
					ts, err := ptypes.TimestampProto(oldNodes.Nodes.GetTimestamp(best))
					if err != nil {
						fmt.Println("Error when converting time.", err.Error())
					}
					repairMsg.Timestamp = ts
					util.SendWithMarshal(queryPort, best, repairMsg)
				}
			}
		}
	}
}

func masterServer() {
	ln, err := net.Listen("tcp", masterPort)
	if err != nil {
		fmt.Printf("Error! Cannot listen on master (id %d): %s\n", myID+1, err.Error())
		return
	}
	defer ln.Close()

	// For loop
	for {
		if !isMaster {
			return
		}
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error! cannot accept when listening:%s\n", err.Error())
			continue
		}

		newMsg, err := util.RecvWithUnmarshal(conn)
		if err != nil {
			continue
		}

		if newMsg.GetAck() {
			masterHandleAck(newMsg)
		} else {
			masterHandleCmd(newMsg)
		}
		// printMasterTable(masterTable, masterID)
	}
}

func sendBackupMsg(port string, mID int) error {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b) // write to buffer b

	err := enc.Encode(masterTable)
	if err != nil {
		log.Fatal("encode error:", err)
		return err
	}

	backupMsg := &fileTransfer.FileTransfer{}
	backupMsg.Source = uint32(myID)
	backupMsg.Command = BACKUP
	backupMsg.AllMetadata = b.Bytes()
	// fmt.Println("Backup sending to node ", mID+1, "from", myID+1)
	return util.SendWithMarshal(port, mID, backupMsg)
}

/* Handle ACK message and update master table */
func masterHandleAck(ackMsg *fileTransfer.FileTransfer) {
	f := ackMsg.GetSdfsFilename()
	cmd := ackMsg.GetCommand()
	sourceID := int(ackMsg.GetSource())

	// If the message is ack, it can only be the ack of:
	// PUT, DELETE
	switch cmd {
	case PUT:
		fmt.Println(time.Since(reReplication))
		reReplication = time.Time{}
		ts, err := ptypes.Timestamp(ackMsg.GetTimestamp())
		if err != nil {
			fmt.Println("Error converting timestamp.", err.Error())
		}
		info := masterTable[f]
		if info.Nodes == nil {
			info.Nodes = make(pq, 0)
		}
		info.Nodes.TryUpdate(sourceID, ts)

		// Remove entry if greater than replicaNum
		if info.Nodes.Len() > 3 {
			var valid map[int]bool
			valid = make(map[int]bool)
			targetIdx := util.HashToVMIdx(f)
			for i := 0; i < replicaNum; i++ {
				for !sMembershipList[targetIdx] {
					targetIdx++
					if targetIdx >= nodeNum {
						targetIdx %= 10
					}
				}
				valid[int(targetIdx)] = true
				targetIdx++
				targetIdx = uint32(util.ModLength(int(targetIdx), nodeNum))
			}
			var extra []int
			for _, nodes := range info.Nodes {
				if valid[nodes.NodeID] == false {
					extra = append(extra, nodes.NodeID)
				}
			}
			for _, extraNode := range extra {
				info.Nodes.Remove(extraNode)
				dMsg := &fileTransfer.FileTransfer{}
				dMsg.Source = uint32(myID)
				dMsg.SdfsFilename = f
				dMsg.Command = DELETE
				util.SendWithMarshal(queryPort, extraNode, dMsg)
			}
		}

		masterTable[f] = info
	case DELETE:
		_, exists := masterTable[f]
		if exists {
			info := masterTable[f]
			if info.Nodes != nil {
				info.Nodes.Remove(sourceID)
			}
			masterTable[f] = info
			if info.Nodes.Len() == 0 {
				delete(masterTable, f)
			}
		} else {
			fmt.Println("File does not exists in master table already.")
		}
	}
}

/* Handle regular commands and update master table */
func masterHandleCmd(msg *fileTransfer.FileTransfer) {

	switch msg.GetCommand() {
	case PUT:
		masterHandleCmdPUT(msg)
	case GET:
		masterHandleCmdGET(msg)
	case DELETE:
		masterHandleCmdDELETE(msg)
	case LS:
		masterHandleCmdLS(msg)
	case BACKUP:
		masterHandleCmdBACKUP(msg)
	}
}

func masterHandleCmdBACKUP(msg *fileTransfer.FileTransfer) {
	rd := bytes.NewReader(msg.GetAllMetadata())
	dec := gob.NewDecoder(rd)
	masterTable = make(map[string]masterFileInfo)
	err := dec.Decode(&masterTable)
	if err != nil {
		log.Fatal("decode error 1:", err)
	}
	// printMasterTable(masterTable, masterID)
}

func masterHandleCmdPUT(msg *fileTransfer.FileTransfer) {
	f := msg.GetSdfsFilename()
	putFile := masterTable[f]
	ts, err := ptypes.Timestamp(msg.GetTimestamp())
	if err != nil {
		fmt.Println(msg)
		fmt.Println("Error converting time.", err.Error())
	}
	writeConflict := false
	if !putFile.LastPUT.IsZero() && ts.Before(putFile.LastPUT.Add(time.Minute)) {
		writeConflict = true
	}
	putFile.LastPUT = ts
	masterTable[f] = putFile
	myMsg := &fileTransfer.FileTransfer{}
	myMsg.Source = uint32(myID)
	myMsg.Command = PUT
	myMsg.SdfsFilename = f
	myMsg.WriteConflict = writeConflict
	util.SendWithMarshal(listenPortPUT, int(msg.GetSource()), myMsg)
}

func masterReturnTargets(fName string, mt map[string]masterFileInfo) []uint32 {
	fileInfo := mt[fName]
	var targets []uint32
	if fileInfo.Nodes != nil {
		for idx := 0; idx < fileInfo.Nodes.Len() && idx < replicaNum; idx++ {
			targets = append(targets, uint32(fileInfo.Nodes[idx].NodeID))
		}
	}
	return targets
}

func masterHandleCmdGET(msg *fileTransfer.FileTransfer) {
	f := msg.GetSdfsFilename()

	getReply := &fileTransfer.FileTransfer{}
	getReply.Source = uint32(myID)
	getReply.Command = GET
	getReply.Nodes = masterReturnTargets(f, masterTable)

	util.SendWithMarshal(listenPortTargets, int(msg.GetSource()), getReply)

}

func masterHandleCmdDELETE(msg *fileTransfer.FileTransfer) {
	deleteReply := &fileTransfer.FileTransfer{}
	deleteReply.Source = uint32(myID)
	deleteReply.Command = DELETE
	deleteReply.Nodes = masterReturnTargets(msg.GetSdfsFilename(), masterTable)

	util.SendWithMarshal(listenPortTargets, int(msg.GetSource()), deleteReply)
}

func masterHandleCmdLS(msg *fileTransfer.FileTransfer) {
	sendBackupMsg(listenPortLS, int(msg.GetSource()))
}

func backupServer() {
	ln, err := net.Listen("tcp", masterPort)
	if err != nil {
		fmt.Printf("Error! Cannot listen on master (id %d): %s", myID+1, err.Error())
		return
	}
	defer ln.Close()

	// For loop
	for {
		if !isMasterBackup {
			return
		}
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error! cannot accept when listening:%s\n", err.Error())

			continue
		}

		newMsg, err := util.RecvWithUnmarshal(conn)
		if err != nil {
			continue
		}
		if newMsg.GetCommand() == BACKUP && int(newMsg.GetSource()) > myID {
			masterHandleCmdBACKUP(newMsg)
		}
	}
}

func checkIsMaster(myID int, nodesAlive [nodeNum]bool, mIDptr *int) (bool, bool) {
	master := true
	backup := false
	var idx int
	for idx = (nodeNum - 1); idx > myID; idx-- {
		if nodesAlive[idx] {
			if master {
				*mIDptr = idx
			}
			master = false
			distance := 1
			for temp := idx - 1; temp >= myID; temp-- {
				if distance > 2 {
					break
				}
				if nodesAlive[temp] {
					if temp == myID {
						backup = true
						break
					}
					distance++
				}
			}
			break
		}
	}
	if idx == myID {
		*mIDptr = myID
	}
	return master, backup
}

func printMasterTable(mt map[string]masterFileInfo, mID int) {
	fmt.Println("Master server ID is:", mID+1)
	fmt.Printf("SDFS filename\tLastUpdate\t\t\t Replica Nodes\n")
	for key, value := range mt {
		fmt.Printf("%s\t\t%s\t\t", key, value.LastPUT.Format(time.UnixDate))
		replicaNodes := masterReturnTargets(key, mt)
		for _, node := range replicaNodes {
			fmt.Printf("%d ", node+1)
		}
		fmt.Printf("\n")
	}
}
