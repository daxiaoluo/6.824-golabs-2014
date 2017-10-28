package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import (
  "sync"
  "strconv"
)

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  store map[string]string
  operations map[int64]string
  view  viewservice.View
  mu    sync.Mutex
}

func (pb *PBServer) isPrimary() bool {
  return pb.me == pb.view.Primary
}

func (pb *PBServer) hasPrimary() bool {
  return pb.view.Primary != ""
}

func (pb *PBServer) isBackup() bool {
  return pb.me == pb.view.Backup
}

func (pb *PBServer) hasBackup() bool {
  return pb.view.Backup != ""
}

func (pb *PBServer) ProcessSnapshot(args *SnapshotArgs, reply *SnapshotReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  if pb.isBackup() {
    pb.store = args.Store
    pb.operations = args.Operations
    reply.Err = OK
    return nil
  } else {
    reply.Err = ErrWrongServer
    return nil
  }
}

func (pb *PBServer) doPut(args *PutArgs, reply *PutReply) {
  if (args.DoHash) {
    reply.PreviousValue = pb.store[args.Key]
    pb.operations[args.Id] = reply.PreviousValue
    pb.store[args.Key] = strconv.Itoa(int(hash(pb.store[args.Key]+ args.Value)))
  } else {
    pb.store[args.Key] = args.Value
    pb.operations[args.Id]= args.Value
  }
}


func (pb *PBServer) ProcessRedirectPut(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if !pb.isBackup() {
    reply.Err = ErrWrongServer
  } else {
    _, ok := pb.operations[args.Id]
    if !ok {
      pb.doPut(args, reply)
    }
    reply.Err = OK
  }
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if !pb.isPrimary() {
    reply.Err = ErrWrongServer
    return nil
  }

  if value, ok := pb.operations[args.Id]; ok {
    if args.DoHash {
      reply.PreviousValue = value
    }
    reply.Err = OK
    return nil
  }

  if pb.hasBackup() {
    ok := call(pb.view.Backup, "PBServer.ProcessRedirectPut", args, reply)
    if !ok || reply.Err != OK {
      reply.Err = ErrWrongServer
      return nil
    }
  }

  pb.doPut(args, reply)
  reply.Err = OK

  return nil
}


func (pb *PBServer) ProcessRedirectGet(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  if !pb.isBackup() {
    reply.Err = ErrWrongServer
  } else {
    v, ok := pb.operations[args.Id]
    if !ok {
      reply.Value = pb.store[args.Key]
      pb.operations[args.Id] = reply.Value
    } else {
      reply.Value = v // This avoid backup no reply to primary then it retry to redirect get
    }
    reply.Err = OK
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  if !pb.isPrimary() {
    reply.Err = ErrWrongServer
    return nil
  }

  if value, ok := pb.operations[args.Id]; ok {
    reply.Value = value
    reply.Err = OK
    return nil
  }

  if pb.hasBackup() {
    ok := call(pb.view.Backup, "PBServer.ProcessRedirectGet", args, reply) // avoid when call backup, the view had been changed
    if !ok || reply.Err != OK {
      reply.Err = ErrWrongServer
      return nil
    } else {
      pb.operations[args.Id] = reply.Value
      return nil
    }
  } else {
    reply.Err = OK
    reply.Value = pb.store[args.Key]
    pb.operations[args.Id] = reply.Value
    return nil
  }
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  v, _ := pb.vs.Ping(pb.view.Viewnum)

  if pb.view.Viewnum != v.Viewnum { // view has been changed
    preView := pb.view
    pb.view = v
    if pb.isPrimary() {
      if pb.hasBackup() {
        snapshotArgs := SnapshotArgs{pb.store, pb.operations}
        snapshotReply := SnapshotReply{}
        ok := call(v.Backup, "PBServer.ProcessSnapshot", &snapshotArgs, &snapshotReply)
        if !ok || snapshotReply.Err != OK { // copy to backup failed, so this view can't be acked
          pb.view = preView
          log.Println("Failed to copy the data to backup(%s)", v.Backup)
        }
      }
    }
  }

}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view = viewservice.View{}
  pb.operations = make(map[int64]string)
  pb.store = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
