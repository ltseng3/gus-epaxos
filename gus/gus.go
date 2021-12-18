package gus

import (
	"dlog"
	//"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	//"io"
	"log"
	"gusproto"
	"state"
	"time"
	"sync"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 1 // No Batch
const MAX_OP = 50000000 // If this is too large, something blows up
//const MAX_TAG = MAX_OP*3
const MAX_TAG = 1000 // 5000*3, MAX_Key = 1000
const MAX_KEY = 5000 // If this is too large, something blows up

//const MAX_WRITE = 5000

var mu  sync.Mutex

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	writeChan           chan fastrpc.Serializable
	ackWriteChan        chan fastrpc.Serializable
	commitWriteChan     chan fastrpc.Serializable
	ackCommitChan       chan fastrpc.Serializable
	updateViewChan      chan fastrpc.Serializable
	readChan            chan fastrpc.Serializable
	ackReadChan         chan fastrpc.Serializable
	writeRPC            uint8
	ackWriteRPC         uint8
	commitWriteRPC      uint8
	ackCommitRPC        uint8
	updateViewRPC       uint8
	readRPC             uint8
	ackReadRPC          uint8
	IsLeader            bool // does this replica think it is the leader
	Shutdown            bool
	counter             int
	flush               bool
	currentTag          map[state.Key]gusproto.Tag //currentTag[i] = tag for object i
	currentSeq          int32
	bookkeeping         []OpsBookkeeping
	storage             map[state.Key]map[gusproto.Tag]state.Value
	tmpStorage          map[state.Key]map[gusproto.Tag]state.Value
	view                map[state.Key]map[gusproto.Tag][]bool //view[i][j][k] = Replica k has object i with tag j
	busyKey             map[state.Key]bool
	pendingOps          []*genericsmr.Propose
}

type OpsBookkeeping struct {
	ackWrites           int
	ackCommits          int
	staleTag            uint8 //match the type in gusproto (no bool field in GoBin)
	maxTime             int32 //match the type of Tag in gusproto
	doneFirstWait       bool  //default = false
	doneSecondWait      bool  //default = false
	ackReads            int
	doneRead            bool
	proposal            *genericsmr.Propose // default = nil
	key                 state.Key
	valueToWrite        state.Value // default = 0
	waitForAckCommit    bool        // default = false
	waitForAckRead      bool        // default = false, decide if need to wait for AckRead msg
	checkStorageForRead bool        // default = false
	complete            bool
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
		0, 0, 0, 0, 0, 0,
		0,
		false,
		false,
		0,
		true,
		//make([]gusproto.Tag, MAX_KEY),
		make(map[state.Key]gusproto.Tag),
		0,
		make([]OpsBookkeeping, MAX_OP),
		make(map[state.Key]map[gusproto.Tag]state.Value),
		make(map[state.Key]map[gusproto.Tag]state.Value),
		make(map[state.Key]map[gusproto.Tag][]bool),
		make(map[state.Key]bool),
		[]*genericsmr.Propose{}}


	r.Durable = durable

	r.writeRPC = r.RegisterRPC(new(gusproto.Write), r.writeChan)
	r.ackWriteRPC = r.RegisterRPC(new(gusproto.AckWrite), r.ackWriteChan)
	r.commitWriteRPC = r.RegisterRPC(new(gusproto.CommitWrite), r.commitWriteChan)
	r.ackCommitRPC = r.RegisterRPC(new(gusproto.AckCommit), r.ackCommitChan)
	r.updateViewRPC = r.RegisterRPC(new(gusproto.UpdateView), r.updateViewChan)
	r.readRPC = r.RegisterRPC(new(gusproto.Read), r.readChan)
	r.ackReadRPC = r.RegisterRPC(new(gusproto.AckRead), r.ackReadChan)

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

	//Initialization
	//Need to move here; otherwise, ConnectToPeers() would time out
	//for i := 0; i < MAX_KEY; i++ {
	//	// Tag
	//	// r.currentTag[i] = gusproto.Tag{0, 0}
	//
	//	// View
	//	r.view[i] = make([]map[int32]bool, MAX_TAG)
	//	for j := 0; j < MAX_TAG; j++{
	//		r.view[i][j] = make(map[int32]bool)
	//	}
	//	for k := 0; k < r.N; k++ {
	//		r.view[i][0][int32(k)] = true
	//	}
	//
	//	// storage's
	//	//r.storage[i] = make(map[gusproto.Tag]state.Value)
	//	//r.tmpStorage[i] = make(map[gusproto.Tag]state.Value)
	//
	//}



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
				mu.Lock()
				r.pendingOps = append(r.pendingOps, propose)
				mu.Unlock()
			}else {

				r.busyKey[key] = true

				r.bookkeeping[r.currentSeq].proposal = propose
				r.bookkeeping[r.currentSeq].key = key

				if propose.Command.Op == state.GET {
					// GET
					fmt.Printf("GUS: Processing Get by Replica %d\n", r.Id)
					r.bookkeeping[r.currentSeq].waitForAckRead = true
					r.bcastRead(r.currentSeq, propose.Command)
					r.currentSeq++
				} else {
					// PUT
					fmt.Printf("GUS: Processing Put by Replica %d\n", r.Id)

					_, existence := r.storage[key]
					if !existence {
						r.storage[key] = make(map[gusproto.Tag]state.Value)
					}

					_, existence = r.tmpStorage[key]
					if !existence {
						r.tmpStorage[key] = make(map[gusproto.Tag]state.Value)
					}

					r.bookkeeping[r.currentSeq].valueToWrite = propose.Command.V
					r.currentTag[key] = gusproto.Tag{r.currentTag[key].Timestamp + 1, r.Id}
					//r.currentTag[key].Timestamp = r.currentTag[key].Timestamp + 1
					r.bcastWrite(r.currentSeq, propose.Command)
					r.currentSeq++

				}

			}
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			onOffProposeChan = nil
			break

		case writeS := <-r.writeChan:
			write := writeS.(*gusproto.Write)
			writeTag := gusproto.Tag{write.CurrentTime, write.WriterID}
			key := write.Command.K
			seq := int32(write.Seq)
			staleTag := uint8(0) // 0 = False

			_, existence := r.currentTag[key]
			if !existence {
				r.currentTag[key] = gusproto.Tag{0, 0}
			}
			currentTag := r.currentTag[key]
			if currentTag.LessThan(writeTag){
				r.currentTag[key] = gusproto.Tag{write.CurrentTime, write.WriterID}
				r.bookkeeping[seq].valueToWrite = write.Command.V

				_, existence := r.storage[key]
				if !existence {
					r.storage[key] = make(map[gusproto.Tag]state.Value)
				}

				r.storage[key][r.currentTag[key]] = write.Command.V
				r.bcastUpdateView(seq, write.WriterID, r.currentTag[key].Timestamp)
			}else {
				_, existence := r.tmpStorage[key]
				if !existence {
					r.tmpStorage[key] = make(map[gusproto.Tag]state.Value)
				}
				r.tmpStorage[key][r.currentTag[key]] = write.Command.V
				staleTag = 1
			}

			fmt.Printf("Replica %d: Received Write from writer %d; value %d; time %d\n", r.Id, write.WriterID, write.Command.V, write.CurrentTime)

			if r.Id == 0 {
				fmt.Printf("Replica %d: +++++++++++++++\n", r.Id)
				for index, element := range r.storage[key] {
					fmt.Println(index.Timestamp, ", ", index.WriterID, "=>", element)
				}
				fmt.Printf("+++++++++++++++\n")
			}
			r.bcastAckWrite(write.Seq, write.WriterID, staleTag, r.currentTag[key])
			break


		case ackWriteS := <-r.ackWriteChan:
			ackWrite := ackWriteS.(*gusproto.AckWrite)
			key := r.bookkeeping[ackWrite.Seq].key

			r.bookkeeping[ackWrite.Seq].ackWrites++
			r.bookkeeping[ackWrite.Seq].staleTag = r.bookkeeping[ackWrite.Seq].staleTag + ackWrite.StaleTag
			if r.bookkeeping[ackWrite.Seq].maxTime < ackWrite.OtherTag.Timestamp {
				r.bookkeeping[ackWrite.Seq].maxTime = ackWrite.OtherTag.Timestamp
			}

			//fmt.Println("GUS: bookKeeping Seq %d with %d ack-write", ackWrite.Seq, r.bookkeeping[ackWrite.Seq].acks)
			if (r.bookkeeping[ackWrite.Seq].ackWrites >= (r.N-1)/2) && !r.bookkeeping[ackWrite.Seq].doneFirstWait && !r.bookkeeping[ackWrite.Seq].complete {
				r.bookkeeping[ackWrite.Seq].doneFirstWait = true
				if r.bookkeeping[ackWrite.Seq].staleTag == 0 {// All staleTag = FALSE
					r.busyKey[key] = false
					// Reply to client
					fmt.Printf("GUS: reply to client %d +++ Fast Path +++\n", r.currentSeq)
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
					r.reset()

					// Tell other replicas to commit
					r.bcastCommitWrite(ackWrite.Seq, ackWrite.WriterID, r.currentTag[key].Timestamp)
					r.bcastUpdateView(ackWrite.Seq, ackWrite.WriterID, r.currentTag[key].Timestamp)

					r.initializeView(key, r.currentTag[key])
					r.view[key][r.currentTag[key]][r.Id] = true
					r.storage[key][r.currentTag[key]] = r.bookkeeping[ackWrite.Seq].valueToWrite
				}else{
					r.currentTag[key] = gusproto.Tag{r.bookkeeping[ackWrite.Seq].maxTime+1, r.Id}
					r.bcastCommitWrite(ackWrite.Seq, ackWrite.WriterID, r.currentTag[key].Timestamp)
					r.bookkeeping[ackWrite.Seq].waitForAckCommit = true
				}
			}
			break

		case commitWriteS := <-r.commitWriteChan:
			commitWrite := commitWriteS.(*gusproto.CommitWrite)
			commitTag := gusproto.Tag{commitWrite.CurrentTime, commitWrite.WriterID}
			key := r.bookkeeping[commitWrite.Seq].key

			if commitTag.GreaterThan(r.currentTag[key]) {
				r.bcastUpdateView(commitWrite.Seq, commitWrite.WriterID, commitWrite.CurrentTime)
				r.currentTag[key] = gusproto.Tag{commitTag.Timestamp, commitTag.WriterID}
			}

			_, existence := r.storage[key]
			if !existence {
				r.storage[key] = make(map[gusproto.Tag]state.Value)
			}
			r.storage[key][r.currentTag[key]] = r.tmpStorage[key][r.currentTag[key]]
			delete(r.tmpStorage[key], r.currentTag[key])
			r.initializeView(key, r.currentTag[key])
			r.view[key][r.currentTag[key]][r.Id] = true
			r.bcastAckCommit(commitWrite.Seq, commitWrite.WriterID)
			break

		case ackCommitS := <-r.ackCommitChan:

			ackCommit := ackCommitS.(*gusproto.AckCommit)
			r.bookkeeping[ackCommit.Seq].ackCommits++
			key := r.bookkeeping[ackCommit.Seq].key

			if r.bookkeeping[ackCommit.Seq].waitForAckCommit && !r.bookkeeping[ackCommit.Seq].complete {
				if (r.bookkeeping[ackCommit.Seq].ackCommits >= (r.N-1)/2) && !r.bookkeeping[ackCommit.Seq].doneSecondWait {
					r.bookkeeping[ackCommit.Seq].doneSecondWait = true
					// Reply to client
					fmt.Printf("GUS: reply to client %d +++ Slow Path +++\n", r.currentSeq)
					if r.bookkeeping[ackCommit.Seq].proposal != nil {
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							r.bookkeeping[ackCommit.Seq].proposal.CommandId,
							state.NIL,
							r.bookkeeping[ackCommit.Seq].proposal.Timestamp}
						r.ReplyProposeTS(propreply, r.bookkeeping[ackCommit.Seq].proposal.Reply)
					}
					r.bookkeeping[ackCommit.Seq].complete = true
					r.bcastUpdateView(ackCommit.Seq, ackCommit.WriterID, r.currentTag[key].Timestamp)
					r.storage[key][r.currentTag[key]] = r.bookkeeping[ackCommit.Seq].valueToWrite
					r.initializeView(key, r.currentTag[key])
					r.view[key][r.currentTag[key]][r.Id] = true
					r.busyKey[key] = false
					r.reset()
				}

			}
			break

		case updateViewS := <-r.updateViewChan:
			//TODO: How to test this???
			updateView := updateViewS.(*gusproto.UpdateView)
			fmt.Printf("GUS: Replica %d: updating view from %d\n", r.Id, updateView.Sender)
			key := r.bookkeeping[updateView.Seq].key

			r.initializeView(key, r.currentTag[key])
			r.view[key][r.currentTag[key]][updateView.Sender] = true

			if r.bookkeeping[updateView.Seq].proposal != nil && !r.bookkeeping[updateView.Seq].complete{
				if (r.bookkeeping[updateView.Seq].proposal.Command.Op == state.GET) && r.bookkeeping[updateView.Seq].checkStorageForRead {

					r.bookkeeping[updateView.Seq].complete = true
					r.busyKey[key] = false
					r.reset()
					tag := gusproto.Tag{r.currentTag[key].Timestamp, r.currentTag[key].WriterID}
					// Reply to client
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						r.bookkeeping[updateView.Seq].proposal.CommandId,
						r.storage[key][tag],
						r.bookkeeping[updateView.Seq].proposal.Timestamp}
					r.ReplyProposeTS(propreply, r.bookkeeping[updateView.Seq].proposal.Reply)
				}

			}
			break


		case readS := <-r.readChan:
			read := readS.(*gusproto.Read)
			r.bcastAckRead(read.Seq, read.ReaderID, read.Command.K)
			break

		case ackReadS := <-r.ackReadChan:
			ackRead := ackReadS.(*gusproto.AckRead)
			key := r.bookkeeping[ackRead.Seq].key

			r.bookkeeping[ackRead.Seq].ackReads++

			currentTag := r.currentTag[key]

			if currentTag.LessThan(ackRead.CurrentTag) {
				r.currentTag[key] = gusproto.Tag{ackRead.CurrentTag.Timestamp, ackRead.CurrentTag.WriterID}
			}

			if (r.bookkeeping[ackRead.Seq].ackReads >= (r.N-1)/2) && r.bookkeeping[ackRead.Seq].waitForAckRead {

				r.initializeView(key, r.currentTag[key])
				if len(r.view[key][r.currentTag[key]]) >= (r.N-1)/2 {
					tag := gusproto.Tag{r.currentTag[key].Timestamp, r.currentTag[key].WriterID}
					// Reply to client
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						r.bookkeeping[ackRead.Seq].proposal.CommandId,
						r.storage[key][tag],
						r.bookkeeping[ackRead.Seq].proposal.Timestamp}
					r.ReplyProposeTS(propreply, r.bookkeeping[ackRead.Seq].proposal.Reply)
					r.bookkeeping[ackRead.Seq].complete = true
					r.busyKey[key] = false
					r.reset()

					//r.currentSeq++
				}else{
					r.bookkeeping[ackRead.Seq].checkStorageForRead = true
				}
				r.bookkeeping[ackRead.Seq].waitForAckRead = false
			}

			break
		}
	}
}

func (r *Replica) initializeView(key state.Key, tag gusproto.Tag) {
	_, existence := r.view[key]
	if !existence {
		r.view[key] = make(map[gusproto.Tag][]bool)
		//for j := 0; j < MAX_TAG; j++ {
		//	r.view[key][j] = make(map[int32]bool)
		//}
		defaultTag := gusproto.Tag{0, 0}
		r.view[key][defaultTag] = make([]bool, r.N)
		for k := 0; k < r.N; k++ {
			r.view[key][defaultTag][int32(k)] = true
		}
	}
	_, existence2 := r.view[key][tag]
	if !existence2 {
		r.view[key][tag] = make([]bool, r.N)
	}
}

func (r *Replica) reset(){
	mu.Lock()
	if len(r.pendingOps) != 0 {
		oldProposal := r.pendingOps[0]
		r.pendingOps = r.pendingOps[1:]
		r.ProposeChan <- oldProposal
		fmt.Println(len(r.pendingOps))
	}
	mu.Unlock()
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

var readMSG gusproto.Read
func (r *Replica) bcastRead(seq int32, command state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	readMSG.Seq = seq
	readMSG.ReaderID = r.Id
	readMSG.Command = command
	args := &readMSG

	r.bcastAll(r.readRPC, args)
}

var ackReadMSG gusproto.AckRead
func (r *Replica) bcastAckRead(seq int32, readerID int32, key state.Key) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	ackReadMSG.Seq = seq
	ackReadMSG.ReaderID = readerID
	ackReadMSG.CurrentTag.Timestamp = r.currentTag[key].Timestamp
	ackReadMSG.CurrentTag.WriterID = r.currentTag[key].WriterID
	args := &ackReadMSG

	r.bcastAll(r.ackReadRPC, args)
}

var writeMSG gusproto.Write
func (r *Replica) bcastWrite(seq int32, command state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	writeMSG.Seq = seq
	writeMSG.WriterID = r.Id
	writeMSG.CurrentTime = r.currentTag[r.bookkeeping[seq].key].Timestamp
	writeMSG.Command = command
	args := &writeMSG

	r.bcastAll(r.writeRPC, args)
}

var ackWriteMSG gusproto.AckWrite
func (r *Replica) bcastAckWrite(seq int32, writerID int32, staleTag uint8, tag gusproto.Tag) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	ackWriteMSG.Seq = seq
	ackWriteMSG.WriterID = writerID
	ackWriteMSG.StaleTag = staleTag // 0 = false
	ackWriteMSG.OtherTag = tag

	args := &ackWriteMSG

	// Send ACK-Write to WriterID
	r.SendMsg(writerID, r.ackWriteRPC, args)

}


var commitWriteMSG gusproto.CommitWrite
func (r *Replica) bcastCommitWrite(seq int32, writerID int32, timestamp int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	commitWriteMSG.Seq = seq
	commitWriteMSG.WriterID = writerID
	commitWriteMSG.CurrentTime = timestamp

	args := &commitWriteMSG

	r.bcastAll(r.commitWriteRPC, args)
}


var ackCommitMSG gusproto.AckCommit
func (r *Replica) bcastAckCommit(seq int32, writerID int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	ackCommitMSG.Seq = seq
	ackCommitMSG.WriterID = writerID
	args := &ackCommitMSG

	// Send ACK-Commit to writerID
	r.SendMsg(writerID, r.ackCommitRPC, args)
}


var updateViewMSG gusproto.UpdateView
func (r *Replica) bcastUpdateView(seq int32, writerID int32, timestamp int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	updateViewMSG.Seq = seq
	updateViewMSG.WriterID = writerID
	updateViewMSG.CurrentTime = timestamp
	updateViewMSG.Sender = r.Id

	args := &updateViewMSG

	r.bcastAll(r.updateViewRPC, args)
}


