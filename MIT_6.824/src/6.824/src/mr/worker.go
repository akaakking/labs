package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//59653963647

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// X map number Y reduce number
const M_FILE_NAME = "./mr-tmp/mr-%v-%v"

// X reduce number
const R_FILE_NAME = "mr-out-%v"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var reply Reply
	var finishedJobNumber int

	finishedJobNumber = FRESH

	rand.Seed(time.Now().Unix())

	file, err := os.OpenFile("../util/log"+strconv.Itoa(rand.Int()), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)

	if err != nil {
		log.Println(err)
	} else {
		log.SetOutput(file)
	}

	for {
		log.Println("向master请求任务,并告知完成了job", finishedJobNumber)
		// 发起调用得到任务分配
		if !call("Coordinator.DistributeJobs", finishedJobNumber, &reply) {
			log.Println("rpc请求失败")
			return
		}

		if reply.TaskNumber == -1 {
			reply.TaskNumber = 0
		}

		log.Println("rpc请求成功,得到reply:", reply)

		// 根据任务类型干活
		switch reply.JobType {
		case EMPTY:
			log.Println("master返回空,任务结束")
			return
		case MAP:
			// do map job
			doMapJob(mapf, &reply)
		case REDUCE:
			// do reduce job
			doReduceJob(reducef, &reply)
		}

		finishedJobNumber = reply.TaskNumber
	}
}

// 读取nReduce个文件的内容 之后进行shuffile,写入一个文件
func doReduceJob(reducef func(string, []string) string, reply *Reply) {
	decs := make([]*json.Decoder, reply.MapCount)

	//log.Println("执行ReduceJob", reply.TaskNumber)
	for i := 0; i < reply.MapCount; i++ {
		fileName := fmt.Sprintf(M_FILE_NAME, i, reply.TaskNumber)
		//log.Println("打开文件", fileName, "进行decode")
		file, err := os.Open(fileName)

		if err != nil {
			//fmt.Println("中间文件打开发生错误")
		}
		defer file.Close()

		decs[i] = json.NewDecoder(file)
	}

	var kva []KeyValue

	for _, dec := range decs {
		for {
			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				//log.Println("decode完成")
				break
			}

			kva = append(kva, kv)
		}
	}

	// 打开输出的文件
	outFileName := fmt.Sprintf(R_FILE_NAME, reply.TaskNumber)

	outFile, err := os.Create(outFileName)
	//log.Println("创造最后输出文件", outFileName)

	if err != nil {
		return
	}

	defer outFile.Close()

	// 排序kva数组
	sort.Sort(ByKey(kva))

	// 归类key交给reducef得到 kv写入文件,双指针法
	i := 0
	var temstrs []string
	for j := 0; j < len(kva); j++ {
		if kva[i].Key == kva[j].Key {
			temstrs = append(temstrs, kva[j].Value)
		} else {
			re := reducef(kva[i].Key, temstrs)
			fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, re)
			temstrs = temstrs[:0]
			i = j
			temstrs = append(temstrs, kva[i].Value)
		}
	}
}

func doMapJob(mapf func(string, string) []KeyValue, reply *Reply) {
	file, err := os.Open(reply.FileName)
	//log.Println("接收到任务", reply.TaskNumber, reply.FileName)
	if err != nil {
		fmt.Printf("打开文件失败", err)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Println("打开文件"+reply.FileName+",读取content失败", err)
	}

	file.Close()

	kva := mapf(reply.FileName, string(content))

	partition(kva, reply)
}

func partition(kva []KeyValue, reply *Reply) {
	encs := make([]*json.Encoder, reply.ReduceCount)

	for i := 0; i < reply.ReduceCount; i++ {
		fileName := fmt.Sprintf(M_FILE_NAME, reply.TaskNumber, i)
		file, err := os.Create(fileName)

		defer file.Close()
		if err != nil {
			//fmt.Println("creat file " + fileName + "错误")
		} else {
			//log.Println("创建了临时文件", fileName)
		}
		encs[i] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % reply.ReduceCount
		encs[index].Encode(&kv)
	}

	//log.Println("MapJob已完成", reply.TaskNumber)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
