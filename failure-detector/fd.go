package fd

import (
	heartbeat "cs425_mp4/protocol-buffer/fd-heartbeat"
	util "cs425_mp4/utility"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	gtimestamp "github.com/golang/protobuf/ptypes/timestamp"
)

const (
	introducerID = 0
	nodeName     = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
	port         = ":3333"
	listLength   = 10
	targetNum    = 4
	alive        = heartbeat.Member_ALIVE
	leave        = heartbeat.Member_LEAVE
	crash        = heartbeat.Member_CRASH
	start        = heartbeat.Member_START
	succesor     = 1
	predecessor  = -1
	invalidEntry = 0
)

type (
	member    = heartbeat.Member
	timestamp struct {
		localTime time.Time
	}
	neighbor struct {
		nodeID int
		// kind: 1 - represents succesor
		// 		 0 - not a valid entry
		//		-1 represents predecesors
		kind int
		addr string
	}
)

var (
	myID           = util.GetIDFromHostname()
	vmID           = myID + 1 // node ID on VM starts from 1
	myIPAdder      net.IP
	myHeartbeat    uint64
	fileName       = fmt.Sprintf("./memeberList-vm%02d.log", vmID)
	membershipList []*member
	allMembership  = &heartbeat.MembershipList{Source: uint32(myID)} //membership
	neighborList   []neighbor
	myTimestamps   []timestamp
	ticker         *time.Ticker
	myLog          *log.Logger
	iHaveLeft      = false
)

func sendMsg() {
	if iHaveLeft {
		// Do nothing if the node has left
		time.Sleep(time.Nanosecond)
		return
	}

	myHeartbeat++ //increment Heartbeat
	membershipList[myID].HeartbeatCount = myHeartbeat

	// Marshal membership list and send
	allMembership.Members = membershipList
	hb, err := proto.Marshal(allMembership) // to binary
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}

	// Send message to the node itself to handle the case where only one node is alive
	dummyMsg := &heartbeat.MembershipList{Source: uint32(myID)}
	dummyHb, err := proto.Marshal(dummyMsg)
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}
	conn, err := net.Dial("udp", fmt.Sprintf(nodeName, myID+1, port))
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}
	defer conn.Close()
	conn.Write(dummyHb)

	numNeighbor := 0

	for i := 0; i < targetNum; i++ {
		if neighborList[i].kind != invalidEntry {
			conn, err := net.Dial("udp", neighborList[i].addr)
			if err != nil {
				fmt.Printf("error has occured! %s\n", err)
				return
			}
			defer conn.Close()
			conn.Write(hb)
			numNeighbor++
		}
	}

	if numNeighbor < targetNum {
		// send to all other nodes if it's introducer
		// introducerJoin()
		introducerSelfJoin()
	}
}

// GetOutboundIP get preferred outbound ip of this machine
// ref: https://stackoverflow.com/a/37382208
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func uint32ToIP(value uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, value)
	return ip
}

func PrintMembershipList() {
	fmt.Printf("ID    Status      Hearbeat    Time last joined                Local Time                      IP\n")
	for idx := 0; idx < listLength; idx++ {
		var status string
		switch membershipList[idx].GetStatus() {
		case alive:
			status = "ALIVE"
		case leave:
			status = "LEAVE"
		case crash:
			status = "CRASH"
		case start:
			status = "START"
		default:
			status = "UNKOWN"
		}
		fmt.Printf("%2d    %s   %12d    %s    %s    %s\n", membershipList[idx].GetId()+1, status, membershipList[idx].GetHeartbeatCount(), convertTime(membershipList[idx].LastJoin).Format(time.UnixDate), myTimestamps[idx].localTime.Format(time.UnixDate), uint32ToIP(membershipList[idx].GetIpAddr()).String())
	}
	fmt.Println()

}

func convertTime(t *gtimestamp.Timestamp) time.Time {
	ts, err := ptypes.Timestamp(t)
	if err != nil {
		return time.Time{}
	}
	return ts
}

func PrintNeighborList() {
	fmt.Println("ID    kind    addr")
	for idx := 0; idx < targetNum; idx++ {
		fmt.Printf("%2d    %4d    %s\n", neighborList[idx].nodeID+1, neighborList[idx].kind, neighborList[idx].addr)
	}
	fmt.Println()
}

func PrintSelfID() {
	fmt.Println("Self ID:", myID+1, "IP:", myIPAdder.String())
	fmt.Println()
}

func NodeJoin() {
	if iHaveLeft == false {
		fmt.Println("This node (", myID+1, ") has already joined.")
	} else {
		initialize()
		iHaveLeft = false
		fmt.Println("This node (", myID+1, ") joined.")
	}
	fmt.Println()
}

func NodeLeave() {
	if iHaveLeft {
		fmt.Println("This node (", myID+1, ") has already left.")
	} else {
		membershipList[myID].Status = leave
		sendMsg()
		iHaveLeft = true
		fmt.Println("This node (", myID+1, ") voluntarily left.")
	}
	fmt.Println()
}

func handleUserInput() {
	for {
		fmt.Println("Please enter \"list\", \"neighbor\", \"id\", \"join\" or \"leave\":")
		var input string
		fmt.Scanln(&input)
		switch input {
		case "list":
			PrintMembershipList()
		case "neighbor":
			PrintNeighborList()
		case "id":
			PrintSelfID()
		case "join":
			NodeJoin()
		case "leave":
			NodeLeave()
		default:
			fmt.Println("Incorrect input. Please try again.")
		}
	}
}

func updateMembershipLists(newHeartbeat *heartbeat.MembershipList) {
	newList := newHeartbeat.Members
	if newHeartbeat.Source != uint32(myID) {
		if (membershipList[introducerID].GetStatus() != alive) && (membershipList[introducerID].GetStatus() != start) && (newHeartbeat.Source == introducerID) {
			membershipList[introducerID].Status = alive
			membershipList[introducerID].HeartbeatCount = newList[introducerID].GetHeartbeatCount()
			membershipList[introducerID].LastJoin = newList[introducerID].GetLastJoin()
			membershipList[introducerID].IpAddr = newList[introducerID].GetIpAddr()
			myTimestamps[introducerID].localTime = time.Now()
			myLog.Printf("New member enter. Node id is %d from ip: %s.\n", introducerID, uint32ToIP(membershipList[introducerID].IpAddr).String())
		} else {
			for i := 0; i < listLength; i++ {
				if i == myID {
					continue
				}
				msgLastJoin := convertTime(newList[i].GetLastJoin())
				localLastJoin := convertTime(membershipList[i].GetLastJoin())
				if msgLastJoin.After(localLastJoin) {
					membershipList[i].Status = newList[i].GetStatus()
					membershipList[i].HeartbeatCount = newList[i].HeartbeatCount
					membershipList[i].LastJoin = newList[i].GetLastJoin()
					membershipList[i].IpAddr = newList[i].GetIpAddr()
					myTimestamps[i].localTime = time.Now()

					if membershipList[i].Status != newList[i].GetStatus() {
						if membershipList[i].Status == crash {
							myLog.Printf("Node %d crashed (by communication).\n", i)
						} else if membershipList[i].Status == leave {
							myLog.Printf("Node %d voluntarily left.\n", i)
						} else if membershipList[i].Status == alive {
							myLog.Printf("New member enter. Node id is %d from ip: %s.\n", i, uint32ToIP(membershipList[i].IpAddr).String())
						}
					}

				} else if (newList[i].GetHeartbeatCount() > membershipList[i].HeartbeatCount) && msgLastJoin.Equal(localLastJoin) {

					if newList[i].GetStatus() == alive && membershipList[i].Status != alive {
						//fmt.Println(convertTime(newList[i].GetLastJoin()))
						membershipList[i].LastJoin = newList[i].GetLastJoin()
						membershipList[i].IpAddr = newList[i].GetIpAddr()
						myLog.Printf("New member enter. Node id is %d from ip: %s.\n", i, uint32ToIP(membershipList[i].IpAddr).String())
					}
					if membershipList[i].Status != newList[i].GetStatus() {
						if membershipList[i].Status == crash {
							myLog.Printf("Node %d crashed (by communication).\n", i)
						} else if membershipList[i].Status == leave {
							myLog.Printf("Node %d voluntarily left.\n", i)
						} else if membershipList[i].Status == alive {
							myLog.Printf("New member enter. Node id is %d from ip: %s.\n", i, uint32ToIP(membershipList[i].IpAddr).String())
						}
					}
					membershipList[i].Status = newList[i].GetStatus()
					membershipList[i].HeartbeatCount = newList[i].GetHeartbeatCount()

					myTimestamps[i].localTime = time.Now()

				} else if msgLastJoin.Equal(localLastJoin) && (newList[i].GetHeartbeatCount() == membershipList[i].HeartbeatCount) {
					if newList[i].GetStatus() == crash || newList[i].GetStatus() == leave {
						membershipList[i].Status = newList[i].GetStatus()
						if membershipList[i].Status == crash {
							myLog.Printf("Node %d crashed (by communication).\n", i)
						} else if membershipList[i].Status == leave {
							myLog.Printf("Node %d voluntarily left.\n", i)
						}
					}
				}
			}
		}

	}

	for i := 0; i < targetNum; i++ {
		if neighborList[i].kind != invalidEntry {
			neighborID := neighborList[i].nodeID
			if membershipList[neighborID].Status == alive {
				if time.Now().After(myTimestamps[neighborID].localTime.Add(1950 * time.Millisecond)) {
					membershipList[neighborID].Status = crash
					myLog.Printf("Node %d crashed (by detection).\n", neighborID)
				}
			}
		}
	}
}

func updateNeighborList() {

	// temp:update predecesors
	preIdx := util.ModLength(myID-1, listLength)
	postIdx := util.ModLength(myID+1, listLength)
	neighborNum := 0

	neighborSet := make(map[int]bool)

	for neighborNum < targetNum/2 {
		if preIdx == myID {
			break
		}
		if membershipList[preIdx].Status == alive {
			neighborSet[preIdx] = true
			neighborList[neighborNum].nodeID = preIdx
			neighborList[neighborNum].kind = predecessor
			neighborList[neighborNum].addr = fmt.Sprintf(nodeName, preIdx+1, port)
			neighborNum++
		}
		preIdx = util.ModLength(preIdx-1, listLength)
	}

	for neighborNum < targetNum/2 {
		neighborList[neighborNum].kind = invalidEntry
		neighborNum++
	}

	// temp:update successor
	for neighborNum < targetNum {
		if postIdx == myID {
			break
		}
		if membershipList[postIdx].Status == alive {
			_, exists := neighborSet[postIdx]
			if exists {
				// No need to continue loop since it's already searched
				break
			}
			neighborList[neighborNum].nodeID = postIdx
			neighborList[neighborNum].kind = succesor
			neighborList[neighborNum].addr = fmt.Sprintf(nodeName, postIdx+1, port)
			neighborNum++
		}
		postIdx = util.ModLength(postIdx+1, listLength)
	}

	for neighborNum < targetNum {
		neighborList[neighborNum].kind = invalidEntry
		neighborNum++
	}
}

func listenMsg() {
	// set up udp listener
	pc, err := net.ListenPacket("udp", port)
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		myLog.Fatal(err)
	}
	defer pc.Close()

	// allow enough space for the incoming buffer (10x larger than needed)
	buf := make([]byte, 10*listLength*(4+8+1+4))
	for {
		if iHaveLeft {
			// do not update anything if the node has left
			time.Sleep(time.Nanosecond)
			continue
		}
		// continue listenning
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			fmt.Println("Error: ", err)
			myLog.Fatal(err)
		}
		hbMsg := &heartbeat.MembershipList{}
		if err := proto.Unmarshal(buf[0:n], hbMsg); err != nil {
			fmt.Printf("Failed. Error: %s\n", err)
			myLog.Fatal(err)
			return
		}
		// fmt.Println("n: ", n)
		// fmt.Println(proto.MarshalTextString(hbMsg))
		myLog.Printf("Message sent from node %d (IP: %s).\n", hbMsg.GetSource(), addr.String())
		updateMembershipLists(hbMsg)
		updateNeighborList()
	}
}

// temp:now test 0-4
func tempTest() {
	for i := 0; i < 5; i++ {
		membershipList[i].Status = alive
		membershipList[i+5].Status = crash
	}
	updateNeighborList()
}

func initialize() {
	// Initialize membership list
	myHeartbeat = 0
	membershipList = make([]*member, listLength, listLength)
	myTimestamps = make([]timestamp, listLength, listLength)
	neighborList = make([]neighbor, targetNum, targetNum)
	for i := 0; i < listLength; i++ {
		membershipList[i] = new(member)
		membershipList[i].Id = uint32(i)
	}
	membershipList[myID].Status = alive

	myIPAdder = getOutboundIP()
	if len(myIPAdder) == net.IPv6len {
		membershipList[myID].IpAddr = binary.BigEndian.Uint32(myIPAdder[12:16])
	} else {
		membershipList[myID].IpAddr = binary.BigEndian.Uint32(myIPAdder)
	}

	membershipList[myID].LastJoin = ptypes.TimestampNow()
}

func introducerJoin() {
	membershipList[myID].LastJoin = ptypes.TimestampNow()
	hb, err := proto.Marshal(allMembership) // to binary
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}
	introducerHost := fmt.Sprintf(nodeName, introducerID+1, port)
	conn, err := net.Dial("udp", introducerHost)
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}
	defer conn.Close()
	conn.Write(hb)
}

func introducerSelfJoin() {
	membershipList[myID].LastJoin = ptypes.TimestampNow()
	allMembership.Members = membershipList
	hb, err := proto.Marshal(allMembership) // to binary
	if err != nil {
		fmt.Printf("error has occured! %s\n", err)
		return
	}

	for i := 0; i < listLength; i++ {
		if i == introducerID {
			continue
		}
		conn, err := net.Dial("udp", fmt.Sprintf(nodeName, i+1, port))
		if err != nil {
			fmt.Printf("error has occured! %s\n, cannot connect node %d", err, i)
			continue
		}
		defer conn.Close()
		conn.Write(hb)
	}
}

func Start() {
	// create log file
	logFile, err := os.Create(fileName)
	defer logFile.Close()
	if err != nil {
		fmt.Println("didn't create the log file!" + fileName)
	}
	myLog = log.New(logFile, "[ vm"+strconv.Itoa(vmID)+" ] ", log.LstdFlags) //create new logger

	initialize()
	// tempTest()

	go listenMsg() // open new go routine to listen

	// go handleUserInput()

	// use timer to send heartbeat
	ticker = time.NewTicker(650 * time.Millisecond) // send every ? seconds
	for _ = range ticker.C {
		sendMsg()
	}

}

func main() {
	Start()
}

func MemberStatus() [listLength]bool {
	var stat [listLength]bool
	for idx := 0; idx < listLength; idx++ {
		if membershipList[idx].GetStatus() == alive {
			stat[idx] = true
		}
	}
	return stat
}
