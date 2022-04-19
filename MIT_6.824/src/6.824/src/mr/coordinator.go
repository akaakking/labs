package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	sync.Mutex
	nReduce int
	MapJobs
	ReduceJobs
}

const COMPLETED = int64(1)

var isMapJobAllFinished bool
var isReduceJobAllFinished bool

type ReduceJobs struct {
	times              []int64
	DistributeJobCount int
}

type MapJobs struct {
	jobs               []string
	times              []int64
	DistributeJobCount int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DistributeJobs(args int, reply *Reply) error {
	// 所有任务分配完毕,返回empty让worker停止工作
	log.Println("客户端完成了：job", args, "并请求任务")

	c.Lock()
	if isReduceJobAllFinished {
		reply.JobType = EMPTY
		return nil
	}

	reply.ReduceCount = c.nReduce
	reply.MapCount = len(c.MapJobs.jobs)

	// mapjob还没有分配完毕,先分配mapjob,分配完了分配reduceJob
	if !isMapJobAllFinished {
		reply.JobType = MAP
		c.distributeMapJob(args, reply)
	} else {
		reply.JobType = REDUCE
		c.distributeReduceJob(args, reply)
	}

	c.Unlock()

	log.Println("返回给客户端的是", reply)
	return nil
}

//分发mapjob Task completed took 13 seconds
func (c *Coordinator) distributeMapJob(taskNumber int, reply *Reply) {
	// unfresh 根据tasknumber清空 job对应的 "time"
	if taskNumber != FRESH {
		//log.Println("MapJob", taskNumber, c.MapJobs.jobs[taskNumber], "completed took", time.Now().Unix()-c.MapJobs.times[taskNumber], "seconds")
		c.MapJobs.times[taskNumber] = COMPLETED
	}

	// distribute < 10 直接拿
	// distribute >= 10 查看未完 便利找出未完成的,做个标记再分配,TODO 这里待优化,如果用双链表肯定更好一些
	//c.Lock()
	if c.MapJobs.DistributeJobCount < len(c.MapJobs.jobs) {
		reply.FileName = c.MapJobs.jobs[c.MapJobs.DistributeJobCount]
		reply.TaskNumber = c.MapJobs.DistributeJobCount
		c.MapJobs.times[c.MapJobs.DistributeJobCount] = time.Now().Unix()
		//log.Println("MapJob", c.MapJobs.DistributeJobCount, c.MapJobs.jobs[c.MapJobs.DistributeJobCount], "is distributed")
		c.MapJobs.DistributeJobCount++
	} else {
		//log.Println("MapJob" + "分配完毕,准备扫描超时任务")
		index := c.findTimeOuter()

		if index == -1 {
			isMapJobAllFinished = true
			//log.Println("MapJob all finished//")
			reply.JobType = REDUCE
			c.distributeReduceJob(FRESH, reply)
			return
		}

		reply.FileName = c.MapJobs.jobs[index]
		reply.TaskNumber = index
		//log.Println("没有return")
	}

	//c.Unlock()
}

func (c *Coordinator) findTimeOuter() int {
	if !isMapJobAllFinished {
		for index, preTime := range c.MapJobs.times {
			if preTime != COMPLETED && time.Now().Unix()-preTime > 10 {
				return index
			}
		}
	} else {
		for index, preTime := range c.ReduceJobs.times {
			if preTime != COMPLETED && time.Now().Unix()-preTime > 10 {
				return index
			}
		}
	}

	return -1
}

//分发reducejob
func (c *Coordinator) distributeReduceJob(taskNumber int, reply *Reply) {
	if taskNumber != FRESH {
		//log.Println("ReduceJob", taskNumber, "completed took", time.Now().Unix()-c.ReduceJobs.times[taskNumber], "seconds")
		c.ReduceJobs.times[taskNumber] = COMPLETED
	} else {
		//log.Println("新用户")
	}

	//c.Lock()
	if c.ReduceJobs.DistributeJobCount < c.nReduce {
		if c.ReduceJobs.DistributeJobCount != 0 {
			reply.TaskNumber = c.ReduceJobs.DistributeJobCount
		} else {
			reply.TaskNumber = -1
		}

		c.ReduceJobs.times[c.ReduceJobs.DistributeJobCount] = time.Now().Unix()
		//log.Println("ReduceJob", c.ReduceJobs.DistributeJobCount, "is distributed")
		c.ReduceJobs.DistributeJobCount++
	} else {
		index := c.findTimeOuter()

		if index == -1 {
			isReduceJobAllFinished = true
			//log.Println("ReduceJob all finished")
			reply.JobType = EMPTY
		} else {
			//log.Println("ReduceJob", index, "is distributed again")
			reply.TaskNumber = index
			c.ReduceJobs.times[index] = time.Now().Unix()
		}
	}
	//c.Unlock()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return isReduceJobAllFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce}

	c.MapJobs.jobs = make([]string, 0, len(files))
	c.MapJobs.jobs = append(c.MapJobs.jobs, files...)
	c.MapJobs.times = make([]int64, len(files))
	//log.Println("all the MapJobs", c.MapJobs.jobs)
	//log.Println("ReduceJobCount", c.nReduce)

	c.ReduceJobs.times = make([]int64, nReduce)

	c.MapJobs.DistributeJobCount = 0
	c.ReduceJobs.DistributeJobCount = 0
	isMapJobAllFinished = false
	isReduceJobAllFinished = false

	log.Println("启动rpc服务器")
	c.server()
	return &c
}

// 先学分布式,在根据duboo,seata这些业务,学点框架,再学tomcat就是学个服务器,最后go写jvm

/*
todo list
先学分布式
重构 mutifiletransfer
学习 dubbo seata这些业务,框架
tomcat
go写jvm
mysql
redis
数据结构
*/
