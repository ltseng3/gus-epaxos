package main

import (
	"bufio"
	"flag"
	"fmt"
	"gus-epaxos/src/genericsmrproto"
	"gus-epaxos/src/poisson"
	"gus-epaxos/src/state"
	"gus-epaxos/src/zipfian"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"time"
)

var leaderAddr *string = flag.String("laddr", "10.10.1.2", "Leader address. Defaults to 10.10.1.2")
var leaderPort *int = flag.Int("lport", 7070, "Leader port.")
var serverAddr *string = flag.String("saddr", "", "Server address.")
var serverPort *int = flag.Int("sport", 7070, "Server port.")
var serverID *int = flag.Int("serverID", 0, "Server's ID")
var procs *int = flag.Int("p", 2, "GOMAXPROCS.")
var conflicts *int = flag.Int("c", 0, "Percentage of conflicts. If -1, uses Zipfian distribution.")
var forceLeader = flag.Int("l", -1, "Force client to talk to a certain replica.")
var startRange = flag.Int("sr", 0, "Key range start")
var T = flag.Int("T", 16, "Number of threads (simulated clients).")
var outstandingReqs = flag.Int64("or", 1, "Number of outstanding requests a thread can have at any given time.")
var theta = flag.Float64("theta", 0.99, "Theta zipfian parameter")
var zKeys = flag.Uint64("z", 1e9, "Number of unique keys in zipfian distribution.")
var poissonAvg = flag.Int("poisson", -1, "The average number of microseconds between requests. -1 disables Poisson.")
var percentWrites = flag.Float64("writes", 1, "A float between 0 and 1 that corresponds to the percentage of requests that should be writes. The remainder will be reads.")
var percentRMWs = flag.Float64("rmws", 0, "A float between 0 and 1 that corresponds to the percentage of writes that should be RMWs. The remainder will be regular writes.")
var blindWrites = flag.Bool("blindwrites", false, "True if writes don't need to execute before clients receive responses.")
var singleClusterTest = flag.Bool("singleClusterTest", true, "True if clients run on a VM in a single cluster")
var rampDown *int = flag.Int("rampDown", 5, "Length of the cool-down period after statistics are measured (in seconds).")
var rampUp *int = flag.Int("rampUp", 5, "Length of the warm-up period before statistics are measured (in seconds).")
var timeout *int = flag.Int("timeout", 180, "Length of the timeout used when running the client")

// Information about the latency of an operation
type response struct {
	receivedAt    time.Time
	rtt           float64 // The operation latency, in ms
	commitLatency float64 // The operation's commit latency, in ms
	operation     state.Operation
	replicaID     int
}

// Information pertaining to operations that have been issued but that have not
// yet received responses
type outstandingRequestInfo struct {
	sync.Mutex
	startTimes map[int32]time.Time // The time at which operations were sent out
	operation  map[int32]state.Operation
}

// An outstandingRequestInfo per client thread
var orInfos []*outstandingRequestInfo

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	orInfos = make([]*outstandingRequestInfo, *T)

	readings := make(chan *response, 100000)
	//startTime := rand.New(rand.NewSource(time.Now().UnixNano()))
	experimentStart := time.Now()

	for i := 0; i < *T; i++ { // i is later used as client's id
		log.Println("Connected to node: ", *serverAddr)

		server, err := net.Dial("tcp", fmt.Sprintf("%s:%d", *serverAddr, *serverPort))
		if err != nil {
			log.Fatalf("Error connecting to replica %s:%d\n", *serverAddr, *serverPort)
		}
		reader := bufio.NewReader(server)
		writer := bufio.NewWriter(server)

		orInfo := &outstandingRequestInfo{
			sync.Mutex{},
			make(map[int32]time.Time, *outstandingReqs),
			make(map[int32]state.Operation, *outstandingReqs)}

		if *serverID != 0 { // not already connected to leader
			leader, err := net.Dial("tcp", fmt.Sprintf("%s:%d", *leaderAddr, *leaderPort))
			if err != nil {
				log.Fatalf("Error connecting to replica %s:%d\n", *leaderAddr, *leaderPort)
			}

			lReader := bufio.NewReader(leader)
			lWriter := bufio.NewWriter(leader)

			go simulatedClientWriter(writer, lWriter /* leader writer*/, orInfo, *serverID)
			go simulatedClientReader(lReader, orInfo, readings, *serverID)
			go simulatedClientReader(reader, orInfo, readings, *serverID)
		} else {
			go simulatedClientWriter(writer, nil /* leader writer*/, orInfo, *serverID)
			go simulatedClientReader(reader, orInfo, readings, *serverID)
		}
		//waitTime := startTime.Intn(3)
		//time.Sleep(time.Duration(waitTime) * 100 * 1e6)

		orInfos[i] = orInfo
	}

	if *singleClusterTest {
		printerMultipleFile(readings, *serverID, experimentStart, rampDown, rampUp, timeout)
	} else {
		printer(readings)
	}
}

func simulatedClientWriter(writer *bufio.Writer, lWriter *bufio.Writer, orInfo *outstandingRequestInfo, serverID int) {
	args := genericsmrproto.Propose{0 /* id */, state.Command{state.PUT, 0, 1}, 0 /* timestamp */}
	//args := genericsmrproto.Propose{0, state.Command{state.PUT, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0}

	conflictRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := zipfian.NewZipfianGenerator(*zKeys, *theta)
	poissonGenerator := poisson.NewPoisson(*poissonAvg)
	opRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	queuedReqs := 0 // The number of poisson departures that have been missed
	for id := int32(0); ; id++ {
		args.CommandId = id

		// Determine key
		if *conflicts >= 0 {
			r := conflictRand.Intn(100)
			if r < *conflicts {
				args.Command.K = 42
			} else {
				//args.Command.K = state.Key(*startRange + 43 + int(id % 888))
				args.Command.K = state.Key(int32(*startRange) + 43 + id)
			}
		} else {
			args.Command.K = state.Key(zipf.NextNumber())
		}

		// Determine operation type
		randNumber := opRand.Float64()
		if *percentWrites+*percentRMWs > randNumber {
			if *percentWrites > randNumber {
				if !*blindWrites {
					args.Command.Op = state.PUT // write operation
				} else {
					//args.Command.Op = state.PUT_BLIND
				}
			} else if *percentRMWs > 0 {
				args.Command.Op = state.RMW // RMW operation
			}
		} else {
			args.Command.Op = state.GET // read operation
		}

		if *poissonAvg != -1 {
			time.Sleep(poissonGenerator.NextArrival())
			queuedReqs += 1
		}

		before := time.Now()
		if (args.Command.Op == state.PUT || args.Command.Op == state.RMW) && serverID != 0 { // send RMWs to leader
			lWriter.WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(lWriter)
			lWriter.Flush()
		} else {
			writer.WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(writer)
			writer.Flush()
		}

		orInfo.Lock()
		orInfo.operation[args.CommandId] = args.Command.Op
		orInfo.startTimes[args.CommandId] = before
		orInfo.Unlock()
	}
}

func simulatedClientReader(reader *bufio.Reader, orInfo *outstandingRequestInfo, readings chan *response, leader int) {
	var reply genericsmrproto.ProposeReplyTS

	for {
		time.Sleep(1 * time.Millisecond)
		if err := reply.Unmarshal(reader); err != nil || reply.OK == 0 {
			log.Println(reply.OK)
			log.Println(reply.CommandId)
			log.Println("Error when reading:", err)
			break
		}
		after := time.Now()

		orInfo.Lock()
		before := orInfo.startTimes[reply.CommandId]
		operation := orInfo.operation[reply.CommandId]
		delete(orInfo.startTimes, reply.CommandId)
		orInfo.Unlock()

		rtt := (after.Sub(before)).Seconds() * 1000
		//commitToExec := float64(reply.Timestamp) / 1e6
		commitLatency := float64(0) //rtt - commitToExec
		readings <- &response{
			after,
			rtt,
			commitLatency,
			operation,
			leader}

	}
}

func printer(readings chan *response) {

	lattputFile, err := os.Create("lattput.txt")
	if err != nil {
		log.Println("Error creating lattput file", err)
		return
	}
	//lattputFile.WriteString("# time (ns), avg lat over the past second, tput since last line, total count, totalOrs, avg commit lat over the past second\n")

	latFile, err := os.Create("latency.txt")
	if err != nil {
		log.Println("Error creating latency file", err)
		return
	}
	//latFile.WriteString("# time (ns), latency, commit latency\n")

	startTime := time.Now()

	for {
		time.Sleep(time.Second)

		count := len(readings)
		var sum float64 = 0
		var commitSum float64 = 0
		endTime := time.Now() // Set to current time in case there are no readings
		for i := 0; i < count; i++ {
			resp := <-readings

			// Log all to latency file
			latFile.WriteString(fmt.Sprintf("%d %f %f\n", resp.receivedAt.UnixNano(), resp.rtt, resp.commitLatency))
			sum += resp.rtt
			commitSum += resp.commitLatency
			endTime = resp.receivedAt
		}

		var avg float64
		var avgCommit float64
		var tput float64
		if count > 0 {
			avg = sum / float64(count)
			avgCommit = commitSum / float64(count)
			tput = float64(count) / endTime.Sub(startTime).Seconds()
		}

		totalOrs := 0
		for i := 0; i < *T; i++ {
			orInfos[i].Lock()
			totalOrs += len(orInfos[i].startTimes)
			orInfos[i].Unlock()
		}

		// Log summary to lattput file
		lattputFile.WriteString(fmt.Sprintf("%d %f %f %d %d %f\n", endTime.UnixNano(),
			avg, tput, count, totalOrs, avgCommit))

		startTime = endTime
	}
}

func printerMultipleFile(readings chan *response, replicaID int, experimentStart time.Time, rampDown, rampUp, timeout *int) {
	fileName := fmt.Sprintf("lattput-%d.txt", replicaID)
	lattputFile, err := os.Create(fileName)
	if err != nil {
		log.Println("Error creating lattput file", err)
		return
	}

	fileName = fmt.Sprintf("latFileRead-%d.txt", replicaID)
	latFileRead, err := os.Create(fileName)
	if err != nil {
		log.Println("Error creating latency file", err)
		return
	}
	//latFile.WriteString("# time (ns), latency, commit latency\n")

	fileName = fmt.Sprintf("latFileWrite-%d.txt", replicaID)
	latFileWrite, err := os.Create(fileName)
	if err != nil {
		log.Println("Error creating latency file", err)
		return
	}

	fileName = fmt.Sprintf("latFileRMW-%d.txt", replicaID)
	latFileRMW, err := os.Create(fileName)
	if err != nil {
		log.Println("Error creating latency file", err)
		return
	}

	startTime := time.Now()

	for {
		time.Sleep(time.Second)

		count := len(readings)
		var sum float64 = 0
		var commitSum float64 = 0
		endTime := time.Now() // Set to current time in case there are no readings
		currentRuntime := time.Now().Sub(experimentStart)
		for i := 0; i < count; i++ {
			resp := <-readings
			// Log all to latency file if they are not within the ramp up or ramp down period.
			if *rampUp < int(currentRuntime.Seconds()) && int(currentRuntime.Seconds()) < *timeout-*rampDown {
				if resp.operation == state.GET {
					latFileRead.WriteString(fmt.Sprintf("%d %f %f\n", resp.receivedAt.UnixNano(), resp.rtt, resp.commitLatency))
				} else if resp.operation == state.PUT {
					latFileWrite.WriteString(fmt.Sprintf("%d %f %f\n", resp.receivedAt.UnixNano(), resp.rtt, resp.commitLatency))
				} else { // rmw
					latFileRMW.WriteString(fmt.Sprintf("%d %f %f\n", resp.receivedAt.UnixNano(), resp.rtt, resp.commitLatency))
				}
				sum += resp.rtt
				commitSum += resp.commitLatency
				endTime = resp.receivedAt
			}
		}

		var avg float64
		var avgCommit float64
		var tput float64
		if count > 0 {
			avg = sum / float64(count)
			avgCommit = commitSum / float64(count)
			log.Println("this count", count)
			tput = float64(count) / endTime.Sub(startTime).Seconds()
		}

		totalOrs := 0
		for i := 0; i < *T; i++ {
			orInfos[i].Lock()
			totalOrs += len(orInfos[i].startTimes)
			orInfos[i].Unlock()
		}

		// Log summary to lattput file
		//lattputFile.WriteString(fmt.Sprintf("%d %f %f %d %d %f\n", endTime.UnixNano(), avg, tput, count, totalOrs, avgCommit))
		// Log all to latency file if they are not within the ramp up or ramp down period.
		if *rampUp < int(currentRuntime.Seconds()) && int(currentRuntime.Seconds()) < *timeout-*rampDown {
			lattputFile.WriteString(fmt.Sprintf("%d %f %f %d %d %f\n", endTime.UnixNano(), avg, tput, count, totalOrs, avgCommit))
		}
		startTime = endTime
	}
}
