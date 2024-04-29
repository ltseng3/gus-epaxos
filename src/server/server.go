package main

import (
	"flag"
	"fmt"
	"gus-epaxos/src/epaxos"
	"gus-epaxos/src/fastpaxos"
	"gus-epaxos/src/gpaxos"
	"gus-epaxos/src/gus"
	"gus-epaxos/src/masterproto"
	"gus-epaxos/src/mencius"
	"gus-epaxos/src/paxos"
	"gus-epaxos/src/multipaxos"
	"gus-epaxos/src/multipaxoslease"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "10.10.1.1", "Master address. Defaults to 10.10.1.1.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var doGus *bool = flag.Bool("gus", true, " Use Gus as the replication protocol. Defaults to true.")
var doMencius *bool = flag.Bool("m", false, "Use Mencius as the replication protocol. Defaults to false.")
var doGpaxos *bool = flag.Bool("g", false, "Use Generalized Paxos as the replication protocol. Defaults to false.")
var doEpaxos *bool = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var doFastpaxos *bool = flag.Bool("f", false, "Use Fast Paxos as the replication protocol. Defaults to false.")
var doMultipaxos *bool = flag.Bool("mp", false, "Use Multi Paxos as the replication protocol. Defaults to false.")
var doMultipaxosLease *bool = flag.Bool("mpl", false, "Use Multi Paxos with LEASE as the replication protocol. Defaults to false.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", false, "Execute commands.")
var dreply = flag.Bool("dreply", true, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	replicaId, nodeList := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))

	if *doGus {
		log.Println("Starting Gus replica...")
		rep := gus.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	} else if *doFastpaxos {
		log.Println("Starting Fast Paxos replica...")
		rep := fastpaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	} else if *doMultipaxos {
		log.Println("Starting Multi-Paxos replica...")
		rep := multipaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)	
	} else if *doMultipaxosLease {
		log.Println("Starting Multi-Paxos with LEASE replica...")
		rep := multipaxoslease.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)		
	} else if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *beacon, *durable)
		rpc.Register(rep)
	} else if *doMencius {
		log.Println("Starting Mencius replica...")
		rep := mencius.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	} else if *doGpaxos {
		log.Println("Starting Generalized Paxos replica...")
		rep := gpaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply)
		rpc.Register(rep)
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

func registerWithMaster(masterAddr string) (int, []string) {
	args := &masterproto.RegisterArgs{*myAddr, *portnum}
	var reply masterproto.RegisterReply

	for done := false; !done; {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true
				break
			}
		}
		time.Sleep(1e9)
	}

	return reply.ReplicaId, reply.NodeList
}

func catchKill(interrupt chan os.Signal) {
	signal := <-interrupt

	if signal == syscall.SIGURG {
		fmt.Println("Caught SIGURG, proceeding with run.")
		return
	}

	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
