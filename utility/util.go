package util

import (
	"bytes"
	"cs425_mp4/protocol-buffer/file-transfer"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
)

const (
	nodeName = "fa17-cs425-g28-%02d.cs.illinois.edu%s"
)

// RecvWithUnmarshal copy contents from a connection and unmarshal it
// Assumes the contents are using protocol buffer
func RecvWithUnmarshal(conn net.Conn) (*fileTransfer.FileTransfer, error) {
	newMsg := &fileTransfer.FileTransfer{}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, conn)
	if err != nil {
		fmt.Println("Error copying from connection!")
		return newMsg, err
	}
	if err := proto.Unmarshal(buf.Bytes(), newMsg); err != nil {
		fmt.Printf("Unmarshall failed (master server). Error: %s. Size: %d\n", err, n)
		return newMsg, err
	}
	return newMsg, nil
}

// SendWithMarshal marshals the message and write to the connection
func SendWithMarshal(port string, targetID int, msg *fileTransfer.FileTransfer) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		fmt.Printf("Error has occured when marshalling! %s\n", err)
		return err
	}
	conn, err := net.Dial("tcp", fmt.Sprintf(nodeName, targetID+1, port))
	if err != nil {
		fmt.Printf("Error! %s occured!\n", err.Error())
		return err
	}
	defer conn.Close()
	n, err := conn.Write(msgBytes)
	if err != nil {
		fmt.Println("Cannot write to connection.", err.Error(), "Wrote", n, "bytes.")
	}
	return err
}

// HashFilenameToVMIdx return the hash value of input string
func HashToVMIdx(filename string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(filename))
	fmt.Printf("hash to : %d\n", h.Sum32()%10)
	return h.Sum32() % 10
}

// GetIDFromHostname return the index of the VM of CS425 (group 28)
func GetIDFromHostname() int {
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

// ModLength returns the modulus in postive number
// Assumes mod is a postive integer
func ModLength(value int, mod int) int {
	result := value % mod
	if result < 0 {
		result += mod
	}
	return result
}

// HostnameStr returns hostname given node id (0-indexed) and port
func HostnameStr(id int, port string) string {
	return fmt.Sprintf(nodeName, id, port)
}
