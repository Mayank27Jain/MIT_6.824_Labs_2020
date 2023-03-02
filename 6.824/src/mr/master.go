package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	nMap        int
	nReduce     int
	mapTasks    []int
	reduceTasks []int
	ogFiles     []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (m *Master) GetJob(args *JobArgs, reply *JobResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	todo := 0
	for i := 0; i < m.nMap; i++ {
		if m.mapTasks[i] < 2 {
			todo = 1
			break
		}
	}
	if todo == 1 {
		var index int
		for index = 0; index < m.nMap; index++ {
			if m.mapTasks[index] == 0 {
				break
			}
		}
		if index == m.nMap {
			reply.Filename = ""
			reply.MorR = 0
			reply.OtherMorR = 0
			reply.Fileid = 0
		} else {
			m.mapTasks[index] = 1
			reply.Filename = m.ogFiles[index]
			reply.MorR = 1
			reply.OtherMorR = m.nReduce
			reply.Fileid = index
			go IAmWaiter(m, index, 1)
		}
	} else {
		for i := 0; i < m.nReduce; i++ {
			if m.reduceTasks[i] < 2 {
				todo = 1
				break
			}
		}
		if todo == 0 {
			reply.Filename = ""
			reply.OtherMorR = 0
			reply.MorR = 3
			reply.Fileid = 0
			return nil
		}
		var index int
		for index = 0; index < m.nReduce; index++ {
			if m.reduceTasks[index] == 0 {
				break
			}
		}
		if index == m.nReduce {
			reply.Filename = ""
			reply.MorR = 0
			reply.OtherMorR = 0
			reply.Fileid = 0
		} else {
			m.reduceTasks[index] = 1
			reply.Filename = fmt.Sprintf("%d", index)
			reply.OtherMorR = m.nMap
			reply.MorR = 2
			reply.Fileid = index
			go IAmWaiter(m, index, 2)
		}
	}
	return nil
}

func IAmWaiter(m *Master, index int, MorR int) {
	time.Sleep(time.Second * 10)
	m.mu.Lock()
	defer m.mu.Unlock()
	if MorR == 1 {
		if m.mapTasks[index] != 2 {
			m.mapTasks[index] = 0
		}
	} else {
		if m.reduceTasks[index] != 2 {
			m.reduceTasks[index] = 0
		}
	}
	fmt.Println(time.Now().Format(time.RFC850))
}

func (m *Master) GiveReport(args *ReportArgs, reply *ReportResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.MorR == 1 {
		m.mapTasks[args.Fileid] = 2
	} else {
		m.reduceTasks[args.Fileid] = 2
	}
	reply.Heartbeat = 0
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < m.nReduce; i++ {
		if m.reduceTasks[i] != 2 {
			return false
		}
	}
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTasks = make([]int, m.nMap)
	m.reduceTasks = make([]int, m.nReduce)
	m.ogFiles = make([]string, m.nMap)
	copy(m.ogFiles, files)
	m.server()
	return &m
}
