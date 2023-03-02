package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallGetJob()
		if reply.MorR == 0 {
			time.Sleep(time.Second)
			continue
		}
		if reply.MorR == 3 {
			return
		}
		if reply.MorR == 1 {
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			kvas := make([][]KeyValue, 0)
			for i := 0; i < reply.OtherMorR; i++ {
				kvas = append(kvas, make([]KeyValue, 0))
			}
			for i := 0; i < len(kva); i++ {
				kvas[ihash(kva[i].Key)%reply.OtherMorR] = append(kvas[ihash(kva[i].Key)%reply.OtherMorR], kva[i])
			}
			for i := 0; i < reply.OtherMorR; i++ {
				ofile, _ := os.Create(fmt.Sprintf("mr-temp-%v-%v", reply.Fileid, i))
				enc := json.NewEncoder(ofile)
				for _, kv := range kvas[i] {
					_ = enc.Encode(&kv)
				}
				os.Rename(fmt.Sprintf("mr-temp-%v-%v", reply.Fileid, i), fmt.Sprintf("mr-%v-%v", reply.Fileid, i))
			}
			CallGiveReport(reply.Fileid, reply.MorR)
		} else {
			var kva []KeyValue
			for i := 0; i < reply.OtherMorR; i++ {
				file, _ := os.Open(fmt.Sprintf("mr-%v-%v", i, reply.Fileid))
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			ofile, _ := os.Create(fmt.Sprintf("mr-temp-out-%v", reply.Fileid))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			os.Rename(fmt.Sprintf("mr-temp-out-%v", reply.Fileid), fmt.Sprintf("mr-out-%v", reply.Fileid))
			CallGiveReport(reply.Fileid, reply.MorR)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallGetJob() JobResponse {
	args := JobArgs{}
	args.Heartbeat = 0
	reply := JobResponse{}
	call("Master.GetJob", &args, &reply)
	return reply
}

func CallGiveReport(fileid int, MorR int) {
	args := ReportArgs{}
	args.Fileid = fileid
	args.MorR = MorR
	reply := ReportResponse{}
	call("Master.GiveReport", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
