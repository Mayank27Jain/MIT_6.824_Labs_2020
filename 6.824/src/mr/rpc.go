package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type JobArgs struct {
	Heartbeat int
}

type JobResponse struct {
	MorR      int
	Filename  string
	Fileid    int
	OtherMorR int
}

type ReportArgs struct {
	MorR   int
	Fileid int
}

type ReportResponse struct {
	Heartbeat int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
