package mapreduce

import "container/list"
import "fmt"
//import "time"


type WorkerInfo struct {
	address string
	// You can add definitions here.
	id int	//at any instant, the job(ID) a worker is responsible with
	done bool //false==job failed,true==success
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}


func rpcWorker(mr *MapReduce,addr string,args *DoJobArgs,reply *DoJobReply, jobChannel chan WorkerInfo){
	ret := call(addr,"Worker.DoJob",args,reply)
	if ret{
		jobChannel <- WorkerInfo{addr,args.JobNumber,true}
		mr.registerChannel <- addr		
	} else {
		jobChannel <- WorkerInfo{addr,args.JobNumber,false}
	}
}


func (mr *MapReduce) RunMaster() *list.List {
	mapChannel, reduceChannel := make(chan WorkerInfo,mr.nMap), make(chan WorkerInfo,mr.nReduce)
	for i:=0;i<mr.nMap;i++{
		go rpcWorker(mr,<- mr.registerChannel,&DoJobArgs{mr.file,"Map",i,mr.nReduce},&DoJobReply{false},mapChannel)		
	}
	for i := 0;i<mr.nMap;i++{
		frontMap := <-mapChannel
		if !frontMap.done{
			go rpcWorker(mr,<- mr.registerChannel,&DoJobArgs{mr.file,"Map",frontMap.id,mr.nReduce},&DoJobReply{false},mapChannel)
		}		
	}
	for i:=0;i<mr.nReduce;i++{		
		go rpcWorker(mr,<- mr.registerChannel,&DoJobArgs{mr.file,"Reduce",i,mr.nMap},&DoJobReply{false},reduceChannel)		 
	}
	
	for i := 0;i<mr.nReduce;i++{
		frontReduce := <-reduceChannel
		if !frontReduce.done{
			go rpcWorker(mr,<- mr.registerChannel,&DoJobArgs{mr.file,"Reduce",frontReduce.id,mr.nMap},&DoJobReply{false},reduceChannel)
		} 
	}
	
	return mr.KillWorkers()
}
