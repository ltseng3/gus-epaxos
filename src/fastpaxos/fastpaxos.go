package fastpaxos

import (
	"dlog"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"math"
	"log"
	"fastpaxosproto"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 1 // No Batch
const MAX_OP = 500000 // If this is too large, something blows up
//const MAX_TAG = MAX_OP*3
const MAX_TAG = 1000 // 5000*3, MAX_Key = 1000
const MAX_KEY = 5000 // If this is too large, something blows up

//const MAX_WRITE = 5000

type Replica struct {
	*genericsmr.Replica// extends a generic Paxos replica
	writeChan           chan fastrpc.Serializable
	ackWriteChan        chan fastrpc.Serializable
	commitWriteChan     chan fastrpc.Serializable
	ackCommitChan     chan fastrpc.Serializable
	readChan            chan fastrpc.Serializable
	ackReadChan         chan fastrpc.Serializable
	acceptChan          chan fastrpc.Serializable
	ackAcceptChan       chan fastrpc.Serializable
	writeRPC            uint8
	ackWriteRPC         uint8
	commitWriteRPC      uint8
	ackCommitRPC		uint8
	readRPC             uint8
	ackReadRPC          uint8
	acceptRPC           uint8
	ackAcceptRPC        uint8
	IsLeader            bool // does this replica think it is the leader
	Shutdown            bool
	counter             int
	flush               bool
	currentVersion      map[state.Key]int32 //currentTag[i] = version for object i
	currentSeq          int32
	bookkeeping         []OpsBookkeeping
	storage             map[state.Key]map[int32]state.Value
	busyKey             map[state.Key]bool
	pendingOps          []*genericsmr.Propose
}

type OpsBookkeeping struct {
	ackWrites        int
	ackReads         int
	ackCommits       int
	maxVersion       int32
	proposal         *genericsmr.Propose // default = nil
	key              state.Key
	valueToWrite     state.Value // default = 0
	doneFirstPhase   bool
	doneSecondPhase  bool
	needSecondPhase  bool
	waitForAckCommit bool
	waitForAckRead   bool
	complete         bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0,
		0, 0, 0, 0,
		false,
		false,
		0,
		true,
		make(map[state.Key]int32),
		0,
		make([]OpsBookkeeping, MAX_OP),
		make(map[state.Key]map[int32]state.Value),
		make(map[state.Key]bool),
		[]*genericsmr.Propose{}}

	r.Durable = durable

	r.writeRPC = r.RegisterRPC(new(fastpaxosproto.Write), r.writeChan)
	r.ackWriteRPC = r.RegisterRPC(new(fastpaxosproto.AckWrite), r.ackWriteChan)
	r.commitWriteRPC = r.RegisterRPC(new(fastpaxosproto.CommitWrite), r.commitWriteChan)
	r.ackCommitRPC = r.RegisterRPC(new(fastpaxosproto.AckCommit), r.ackCommitChan)
	r.readRPC = r.RegisterRPC(new(fastpaxosproto.Read), r.readChan)
	r.ackReadRPC = r.RegisterRPC(new(fastpaxosproto.AckRead), r.ackReadChan)

	go r.run()
	return r
}

//sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

/* RPC to be called by master */

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	r.IsLeader = true
	return nil
}


/* ============= */

var clockChan chan bool

func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(1000)
		clockChan <- true
	}
}

/* Main event processing loop */

func (r *Replica) run() {

	r.ConnectToPeers()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	if r.Id == 0 {
		r.IsLeader = true
		//r.currentTag[42].Timestamp = 100 // For testing
	}

	clockChan = make(chan bool, 1)
	go r.clock()

	onOffProposeChan := r.ProposeChan

	for !r.Shutdown {

		select {

		case <-clockChan:
			//activate the new proposals channel
			onOffProposeChan = r.ProposeChan
			break

		case propose := <-onOffProposeChan:
			//got a Propose from a client
			dlog.Printf("Proposal with op %d\n", propose.Command.Op)
			//fmt.Printf("Proposal with op %d\n", propose.Command.Op)

			key := propose.Command.K

			if r.busyKey[key] {
				r.pendingOps = append(r.pendingOps, propose)
			}else {

				r.busyKey[key] = true
				r.bookkeeping[r.currentSeq].proposal = propose
				r.bookkeeping[r.currentSeq].key = key

				_, existence := r.currentVersion[key]
				if !existence {
					r.currentVersion[key] = 0
				}

				r.bookkeeping[r.currentSeq].maxVersion = r.currentVersion[key]

				if propose.Command.Op == state.GET {
					// GET
					//fmt.Printf("Fast Paxos: Processing Get by Replica %d\n", r.Id)
					r.bookkeeping[r.currentSeq].waitForAckRead = true
					r.bcastRead(r.currentSeq, propose.Command)
					r.currentSeq++
				} else {
					// PUT
					//fmt.Printf("Fast Paxos: Processing Put by Replica %d\n", r.Id)

					_, existence2 := r.storage[key]
					if !existence2 {
						r.storage[key] = make(map[int32]state.Value)
					}


					r.bookkeeping[r.currentSeq].valueToWrite = propose.Command.V


					r.currentVersion[key] = r.currentVersion[key] + 1
					r.bookkeeping[r.currentSeq].maxVersion = r.currentVersion[key]
					r.bcastWrite(r.currentSeq, propose.Command)
					r.currentSeq++

				}

			}
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			onOffProposeChan = nil
			break

		case writeS := <-r.writeChan:
			write := writeS.(*fastpaxosproto.Write)
			version := write.Version
			key := write.Command.K
			seq := int32(write.Seq)
			id := write.WriterID


			_, existence := r.currentVersion[key]
			if !existence {
				r.currentVersion[key] = 0
			}
			myVersion := r.currentVersion[key]


			if myVersion < version {
				r.currentVersion[key] = version

				_, existence := r.storage[key]
				if !existence {
					r.storage[key] = make(map[int32]state.Value)
				}
				r.storage[key][version] = write.Command.V
				r.bcastAckWrite(seq, id, version)
			}else{
				r.bcastAckWrite(seq, id, version)
			}

			fmt.Printf("Replica %d: Received Write from writer %d; value %d; time %d\n", r.Id, write.WriterID, write.Command.V, write.Version)

			//if r.Id == 0 {
			//	fmt.Printf("Replica %d: +++++++++++++++\n", r.Id)
			//	for index, element := range r.storage[key] {
			//		fmt.Println(index.Timestamp, ", ", index.WriterID, "=>", element)
			//	}
			//	fmt.Printf("+++++++++++++++\n")
			//}
			break


		case ackWriteS := <-r.ackWriteChan:
			ackWrite := ackWriteS.(*fastpaxosproto.AckWrite)
			key := r.bookkeeping[ackWrite.Seq].key

			r.bookkeeping[ackWrite.Seq].ackWrites++
			if r.bookkeeping[ackWrite.Seq].maxVersion < ackWrite.LatestVersion {
				r.bookkeeping[ackWrite.Seq].maxVersion = ackWrite.LatestVersion
				r.bookkeeping[ackWrite.Seq].needSecondPhase = true
			}

			fastQuorumSize := int(math.Floor(3*float64(r.N)/4))

			//fmt.Println("GUS: bookKeeping Seq %d with %d ack-write", ackWrite.Seq, r.bookkeeping[ackWrite.Seq].acks)
			if (r.bookkeeping[ackWrite.Seq].ackWrites >= fastQuorumSize) && !r.bookkeeping[ackWrite.Seq].doneFirstPhase && !r.bookkeeping[ackWrite.Seq].complete {
				r.bookkeeping[ackWrite.Seq].doneFirstPhase = true
				if !r.bookkeeping[ackWrite.Seq].needSecondPhase {// All staleTag = FALSE

					// Reply to client
					//fmt.Printf("Fasx Paxos: reply to client %d +++ Fast Path +++\n", r.currentSeq)
					if r.bookkeeping[ackWrite.Seq].proposal != nil {
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							r.bookkeeping[ackWrite.Seq].proposal.CommandId,
							state.NIL,
							r.bookkeeping[ackWrite.Seq].proposal.Timestamp}
						r.ReplyProposeTS(propreply, r.bookkeeping[ackWrite.Seq].proposal.Reply)
						r.bookkeeping[ackWrite.Seq].complete = true
					}
					r.busyKey[key] = false
					r.reset(ackWrite.Seq)

					// Tell others to commit
					r.bcastCommitWrite(ackWrite.Seq, ackWrite.WriterID, r.bookkeeping[ackWrite.Seq].maxVersion)
					r.storage[key][r.bookkeeping[ackWrite.Seq].maxVersion] = r.bookkeeping[ackWrite.Seq].valueToWrite
				}else{
					r.currentVersion[key] = r.bookkeeping[ackWrite.Seq].maxVersion + 1
					r.bcastCommitWrite(ackWrite.Seq, ackWrite.WriterID, r.bookkeeping[ackWrite.Seq].maxVersion)
					r.bookkeeping[ackWrite.Seq].waitForAckCommit = true
				}
			}
			break

		case commitWriteS := <-r.commitWriteChan:
			commitWrite := commitWriteS.(*fastpaxosproto.CommitWrite)
			version := commitWrite.Version
			key := commitWrite.Key

			if version > r.currentVersion[key] {
				r.currentVersion[key] = version
			}

			_, existence := r.storage[key]
			if !existence {
				r.storage[key] = make(map[int32]state.Value)
			}
			r.storage[key][r.currentVersion[key]] = commitWrite.Value
			r.bcastAckCommit(commitWrite.Seq, commitWrite.ID)
			break

		case ackCommitS := <-r.ackCommitChan:
			ackCommit := ackCommitS.(*fastpaxosproto.AckCommit)
			seq := ackCommit.Seq

			r.bookkeeping[seq].ackCommits++

			if (r.bookkeeping[seq].ackCommits >= (r.N-1)/2) && r.bookkeeping[seq].needSecondPhase &&
				!r.bookkeeping[seq].complete && r.bookkeeping[seq].waitForAckCommit {
				r.bookkeeping[seq].complete = true
				key := r.bookkeeping[seq].proposal.Command.K
				// Reply to client
				propreply := &genericsmrproto.ProposeReplyTS{
					TRUE,
					r.bookkeeping[seq].proposal.CommandId,
					r.storage[key][r.currentVersion[key]],
					r.bookkeeping[seq].proposal.Timestamp}
				r.ReplyProposeTS(propreply, r.bookkeeping[seq].proposal.Reply)
				r.bookkeeping[seq].complete = true
				r.busyKey[key] = false
				r.reset(seq)
			}

			break

		case readS := <-r.readChan:
			read := readS.(*fastpaxosproto.Read)
			r.bcastAckRead(read.Seq, read.ReaderID, read.Command.K)
			break

		case ackReadS := <-r.ackReadChan:
			ackRead := ackReadS.(*fastpaxosproto.AckRead)
			key := r.bookkeeping[ackRead.Seq].key
			r.bookkeeping[ackRead.Seq].ackReads++

			version := r.currentVersion[key]

			if version < ackRead.Version {//A conflicting write
				r.currentVersion[key] = ackRead.Version
				_, existence := r.storage[key]
				if !existence {
					r.storage[key] = make(map[int32]state.Value)
				}
				r.storage[key][r.currentVersion[key]] = ackRead.Value
				r.bookkeeping[ackRead.Seq].needSecondPhase = true
			}
			fastQuorumSize := int(math.Floor(3*float64(r.N)/4))
			if (r.bookkeeping[ackRead.Seq].ackReads >= fastQuorumSize) && r.bookkeeping[ackRead.Seq].waitForAckRead {
				if r.bookkeeping[ackRead.Seq].needSecondPhase {
					r.bcastCommitWrite(ackRead.Seq, ackRead.ReaderID, r.bookkeeping[ackRead.Seq].maxVersion)
					r.bookkeeping[ackRead.Seq].waitForAckCommit = true
				}else{
					// Reply to client
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						r.bookkeeping[ackRead.Seq].proposal.CommandId,
						r.storage[key][r.currentVersion[key]],
						r.bookkeeping[ackRead.Seq].proposal.Timestamp}
					r.ReplyProposeTS(propreply, r.bookkeeping[ackRead.Seq].proposal.Reply)
					r.bookkeeping[ackRead.Seq].complete = true
					r.bookkeeping[ackRead.Seq].valueToWrite = r.storage[key][r.currentVersion[key]]
					r.busyKey[key] = false
					r.reset(ackRead.Seq)
				}
				r.bookkeeping[ackRead.Seq].waitForAckRead = false
			}

			break
		}
	}
}


// seq denotes is associated with the operation that just completes
func (r *Replica) reset(seq int32){
	// Optimization: process pending operations
	if len(r.pendingOps) != 0 {
		var proposal *genericsmr.Propose
		var newSlice []*genericsmr.Propose


		for i:=0; i < len(r.pendingOps); i++ {
			proposal = r.pendingOps[i]
			if proposal.Command.Op == state.GET{
				propreply := &genericsmrproto.ProposeReplyTS{
					TRUE,
					proposal.CommandId,
					r.bookkeeping[seq].valueToWrite,
					proposal.Timestamp}
				r.ReplyProposeTS(propreply, proposal.Reply)
				//TODO: add this proposal to the booking for debugging/logging purpose
			}else{
				newSlice = append(newSlice, proposal)
			}
			fmt.Print("Handling concurrent operations %d\n", len(r.pendingOps))
		}

		r.pendingOps = newSlice
	}

	if len(r.pendingOps) != 0 {
		oldProposal := r.pendingOps[0]
		r.pendingOps = r.pendingOps[1:]
		r.ProposeChan <- oldProposal
		fmt.Println(len(r.pendingOps))
	}
}


/**********************************************************************
                    inter-replica communication
***********************************************************************/

func (r *Replica) bcastAll(whichRPC uint8, msg fastrpc.Serializable) {

	//n := r.N - 1
	n := r.N - 1
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, whichRPC, msg)
	}
}

var writeMSG fastpaxosproto.Write
func (r *Replica) bcastWrite(seq int32, command state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	writeMSG.Seq = seq
	writeMSG.WriterID = r.Id
	writeMSG.Version = r.currentVersion[r.bookkeeping[seq].key]
	writeMSG.Command = command
	args := &writeMSG

	r.bcastAll(r.writeRPC, args)
}

var ackWriteMSG fastpaxosproto.AckWrite
func (r *Replica) bcastAckWrite(seq int32, writerID int32, version int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	ackWriteMSG.Seq = seq
	ackWriteMSG.WriterID = writerID
	ackWriteMSG.LatestVersion = version

	args := &ackWriteMSG

	// Send ACK-Write to WriterID
	r.SendMsg(writerID, r.ackWriteRPC, args)

}

var commitWriteMSG fastpaxosproto.CommitWrite
func (r *Replica) bcastCommitWrite(seq int32, id int32, version int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	commitWriteMSG.Seq = seq
	commitWriteMSG.ID = id
	commitWriteMSG.Version = version
	commitWriteMSG.Key = r.bookkeeping[seq].proposal.Command.K
	commitWriteMSG.Value = r.bookkeeping[seq].valueToWrite

	args := &commitWriteMSG

	r.bcastAll(r.commitWriteRPC, args)
}


var ackCommitMSG fastpaxosproto.AckCommit
func (r *Replica) bcastAckCommit(seq int32, coordinator int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	ackCommitMSG.Seq = seq
	ackCommitMSG.Coordinator = coordinator
	args := &ackCommitMSG

	// Send ACK-Commit to writerID
	r.SendMsg(coordinator, r.ackCommitRPC, args)
}

var readMSG fastpaxosproto.Read
func (r *Replica) bcastRead(seq int32, command state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	readMSG.Seq = seq
	readMSG.ReaderID = r.Id
	readMSG.Version = r.bookkeeping[r.currentSeq].maxVersion
	readMSG.Command = command
	args := &readMSG

	r.bcastAll(r.readRPC, args)
}

var ackReadMSG fastpaxosproto.AckRead
func (r *Replica) bcastAckRead(seq int32, readerID int32, key state.Key) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	ackReadMSG.Seq = seq
	ackReadMSG.ReaderID = readerID

	version, existence := r.currentVersion[key]
	if !existence {
		r.currentVersion[key] = 0
		version = 0
	}

	ackReadMSG.Version = version

	value, existence2 := r.storage[key][version]
	if !existence2 {
		value = state.Value(0)
	}

	ackReadMSG.Value = value

	args := &ackReadMSG

	r.bcastAll(r.ackReadRPC, args)
}
