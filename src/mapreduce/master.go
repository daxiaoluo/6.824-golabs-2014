package mapreduce
import "container/list"
import "fmt"
//import "time"

type WorkerInfo struct {
  address string
  // You can add definitions here.
  idle bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

//func (mr *MapReduce) dispatchWorker() {
//  for mr.alive {
//    req := <- mr.request
//    findIdel := false
//    for !findIdel {
//      for worker := range mr.Workers {
//        if mr.Workers[worker].idle {
//          mr.Workers[worker].idle = false
//          req <- worker
//          findIdel = true
//          break
//        }
//      }
//    }
//  }
//}

func CreateDoJobArgs(file string, operation JobType, jobNumber int, numOtherPhase int) *DoJobArgs {
  args := &DoJobArgs{}
  args.File = file
  args.Operation = operation
  args.JobNumber = jobNumber
  args.NumOtherPhase = numOtherPhase
  return args
}

func TryDispatchWorker(mr *MapReduce, requests []chan string) []chan string {
  mr.mu.Lock()
  defer mr.mu.Unlock()
  if len(requests) == 0 {
    return requests
  }

  for worker := range mr.Workers {
    if mr.Workers[worker].idle {
      mr.Workers[worker].idle = false
      request := requests[0]
      request <- worker
      return requests[1:]
    }
  }
  return requests
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  DPrintf("RunMaster %s\n", mr.MasterAddress)
  workerGet := make(chan chan string)
  workerPut := make(chan string)
  workerRm := make(chan string)
  go func() {
    queue := []chan string{}
    for {
      select {
      case registedWorkerName := <- mr.registerChannel:
        mr.mu.Lock()
        mr.Workers[registedWorkerName] = &WorkerInfo{registedWorkerName, true}
        mr.mu.Unlock()
        fmt.Printf("Register a new worker:%s\n", registedWorkerName)
        queue = TryDispatchWorker(mr, queue)
      case getWorkerRequest := <- workerGet:
        fmt.Printf("Recieve get worker request\n")
        queue = append(queue, getWorkerRequest)
        queue = TryDispatchWorker(mr, queue)
      case putWorkerName := <- workerPut:
        mr.mu.Lock()
        _, ok := mr.Workers[putWorkerName]
        if !ok {
          fmt.Printf("Return a new worker:%s\n", putWorkerName)
          mr.Workers[putWorkerName] = &WorkerInfo{putWorkerName, true}
        } else {
          fmt.Printf("Worker:%s is return\n", putWorkerName)
          mr.Workers[putWorkerName].idle = true
        }
        mr.mu.Unlock()
        queue = TryDispatchWorker(mr, queue)
      case rmWorkerName := <- workerRm:
        mr.mu.Lock()
        fmt.Printf("RM worker:%s\n", rmWorkerName)
        delete(mr.Workers, rmWorkerName)
        mr.mu.Unlock()
      }
    }
  }()

  //go mr.dispatchWorker()

  mapWaitChannel := make(chan struct{}, mr.nMap)
  reduceWaitChannel := make(chan struct{}, mr.nReduce)

  for i := 0; i < mr.nMap; i++ {
    go func(jobId int) {
      done := false
      for !done {
        req := make(chan string)
        workerGet <- req
        workName := <- req
        fmt.Printf("worker get: %s for map job %d\n", workName, jobId)
        mapJob := CreateDoJobArgs(mr.file, Map, jobId, mr.nReduce)
        reply := &DoJobReply{}
        call(workName, "Worker.DoJob", mapJob, reply)
        if reply.OK {
          fmt.Printf("Map job %d done successfully!\n", jobId)
          workerPut <- workName
          done = true
          mapWaitChannel <- struct {}{}
        } else {
          fmt.Printf("Map job %d failed!\n", jobId)
          workerRm <- workName
        }
      }
    }(i)
  }

  for i := 0; i < mr.nMap; i++ {
    <- mapWaitChannel
  }
  fmt.Printf("Map jobs finished!")

  for i := 0; i < mr.nReduce; i++ {
    go func(jobId int) {
      done := false
      for !done {
        req := make(chan string)
        workerGet <- req
        workName := <- req
        fmt.Printf("worker get: %s for reduce job %d\n", workName, jobId)
        mapJob := CreateDoJobArgs(mr.file, Reduce, jobId, mr.nMap)
        reply := &DoJobReply{}
        call(workName, "Worker.DoJob", mapJob, reply)
        if reply.OK {
          fmt.Printf("Reduce job %d done successfully!\n", jobId)
          workerPut <- workName
          done = true
          reduceWaitChannel <- struct {}{}
        } else {
          fmt.Printf("Reduce job %d failed!\n", jobId)
          workerRm <- workName
        }
      }
    }(i)
  }

  for i := 0; i < mr.nReduce; i++ {
    <- reduceWaitChannel
  }
  fmt.Printf("Reduce jobs finished!")

  return mr.KillWorkers()
}
