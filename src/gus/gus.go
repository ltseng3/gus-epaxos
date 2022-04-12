package gus

import (
	"dlog"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"gusproto"
	"log"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const OptimizedRead = true
const MAX_OP = 50000000 // If this is too large, something blows up

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
	asyncStorage        []*AsyncObj
	tmpAsyncStorage     []*AsyncObj
	view                map[state.Key]map[gusproto.Tag][]bool //view[i][j][k] = Replica k has object i with tag j
	activeRead          map[state.Key]bool
	activeWrite         map[state.Key]bool
	leadingOp           map[state.Key]int32
	pendingReads        []*genericsmr.Propose
	readQurum           int
	writeQuorum         int
}

type AsyncObj struct {
	key   state.Key
	seq   int32
	tag   gusproto.Tag
	value state.Value
}

type OpsBookkeeping struct {
	ackWrites           int
	ackCommits          int
	staleTag            uint8 //match the type in gusproto (no bool field in GoBin)
	maxTime             int32 //match the type of Tag in gusproto
	doneFirstWait       bool  //default = false
	ackReads            int
	doneRead            bool
	proposal            *genericsmr.Propose // default = nil
	key                 state.Key
	valueToWrite        state.Value // default = 0
	waitForAckCommit    bool        // default = false
	waitForAckRead      bool        // default = false, decide if need to wait for AckRead msg
	checkStorageForRead bool        // default = false
	complete            bool
	isAsyncWrite        uint8
	receivedTag         map[gusproto.Tag]int
	myTag               gusproto.Tag
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, readQ int, writeQ int) *Replica {
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
		make(map[state.Key]gusproto.Tag),
		0,
		make([]OpsBookkeeping, MAX_OP),
		make(map[state.Key]map[gusproto.Tag]state.Value),
		make(map[state.Key]map[gusproto.Tag]state.Value),
		[]*AsyncObj{},
		[]*AsyncObj{},
		make(map[state.Key]map[gusproto.Tag][]bool),
		make(map[state.Key]bool),
		make(map[state.Key]bool),
		make(map[state.Key]int32),
		[]*genericsmr.Propose{},
		0,
		0}

	r.Durable = durable
	r.readQurum = readQ
	r.writeQuorum = writeQ

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
		time.Sleep(1e6 * 0.01) // 0.01ms
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

			key := propose.Command.K

			if r.activeRead[key] && propose.Command.Op == state.GET && OptimizedRead {
				// These reads can be optimized by tagging along with prior operations
				r.pendingReads = append(r.pendingReads, propose)

			} else {

				// Initialize bookkeeping struct
				r.bookkeeping[r.currentSeq].proposal = propose
				r.bookkeeping[r.currentSeq].key = key
				r.bookkeeping[r.currentSeq].receivedTag = make(map[gusproto.Tag]int)

				if propose.Command.Op == state.GET {
					// GET
					dlog.Printf("GUS: Processing Get by Replica %d\n", r.Id)
					// Wait for a quorum in the first phase
					r.activeRead[key] = true
					r.bookkeeping[r.currentSeq].waitForAckRead = true
					r.bcastRead(r.currentSeq, propose.Command)
					r.currentSeq++
				} else {
					// PUT
					dlog.Printf("GUS: Processing Put by Replica %d\n", r.Id)

					if r.activeWrite[key] {
						r.bookkeeping[r.currentSeq].isAsyncWrite = uint8(1)
						r.bookkeeping[r.currentSeq].maxTime = r.currentTag[key].Timestamp
					} else {
						// Initialize storage space if key is not already existed
						_, existence := r.storage[key]
						if !existence {
							r.storage[key] = make(map[gusproto.Tag]state.Value)
						}
						_, existence = r.tmpStorage[key]
						if !existence {
							r.tmpStorage[key] = make(map[gusproto.Tag]state.Value)
						}

						// Put value and tag in the bookkeeping
						r.bookkeeping[r.currentSeq].valueToWrite = propose.Command.V
						r.bookkeeping[r.currentSeq].maxTime = r.currentTag[key].Timestamp + 1
						r.currentTag[key] = gusproto.Tag{r.currentTag[key].Timestamp + 1, r.Id}
					}
					r.bookkeeping[r.currentSeq].myTag = gusproto.Tag{r.bookkeeping[r.currentSeq].maxTime, r.Id}
					r.bcastWrite(r.currentSeq, propose.Command, r.bookkeeping[r.currentSeq].isAsyncWrite)
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
			isAsyncWrite := write.IsAsync

			// Default tag is (0, 0)
			_, existence := r.currentTag[key]
			if !existence {
				r.currentTag[key] = gusproto.Tag{0, 0}
			}
			currentTag := r.currentTag[key]
			if isAsyncWrite == 0 {
				// Incoming write has a larger tag
				if currentTag.LessThan(writeTag) {
					// Update tag and initialize storage
					r.currentTag[key] = gusproto.Tag{write.CurrentTime, write.WriterID}
					_, existence2 := r.storage[key]
					if !existence2 {
						r.storage[key] = make(map[gusproto.Tag]state.Value)
					}
					r.initializeView(key, r.currentTag[key])
					r.view[key][r.currentTag[key]][r.Id] = true
					r.storage[key][r.currentTag[key]] = write.Command.V

					// transform write into byte array
					if r.Durable {
						var b [24]byte
						binary.LittleEndian.PutUint64(b[0:8], uint64(key))
						binary.LittleEndian.PutUint32(b[8:12], uint32(r.currentTag[key].Timestamp))
						binary.LittleEndian.PutUint32(b[12:16], uint32(r.currentTag[key].WriterID))
						binary.LittleEndian.PutUint64(b[16:24], uint64(write.Command.V))
						r.StableStore.Write(b[:])
						r.sync()
					}

					r.bcastUpdateView(seq, write.WriterID, r.currentTag[key].Timestamp)
				} else {
					// Incoming write has a smaller tag, so put it in the tmpStorage
					_, existence2 := r.tmpStorage[key]
					if !existence2 {
						r.tmpStorage[key] = make(map[gusproto.Tag]state.Value)
					}
					r.tmpStorage[key][r.currentTag[key]] = write.Command.V
					// Notify writer that it has a stale tag
					staleTag = 1
				}
			} else {
				if currentTag.LessThan(writeTag) {
					r.asyncStorage = append(r.asyncStorage, &AsyncObj{key, seq, writeTag, write.Command.V})
				} else {
					staleTag = 1
					r.tmpAsyncStorage = append(r.tmpAsyncStorage, &AsyncObj{key, seq, writeTag, write.Command.V})
				}
			}
			r.bcastAckWrite(write.Seq, write.WriterID, staleTag, r.currentTag[key])
			break

		case ackWriteS := <-r.ackWriteChan:
			ackWrite := ackWriteS.(*gusproto.AckWrite)
			seq := ackWrite.Seq
			key := r.bookkeeping[seq].key

			r.bookkeeping[seq].ackWrites++
			// Record "larger" tags received from writeQuorum
			if r.bookkeeping[seq].myTag.LessThan(ackWrite.OtherTag) {
				_, existenceTag := r.bookkeeping[seq].receivedTag[ackWrite.OtherTag]
				if !existenceTag {
					r.bookkeeping[seq].receivedTag[ackWrite.OtherTag] = 1
				} else {
					r.bookkeeping[seq].receivedTag[ackWrite.OtherTag] = r.bookkeeping[seq].receivedTag[ackWrite.OtherTag] + 1

					// Decide if there might be a completed write
					// If there is, then set staleTag to 1
					threshold := 2*r.writeQuorum - r.N
					if r.bookkeeping[seq].receivedTag[ackWrite.OtherTag] >= threshold {
						r.bookkeeping[seq].staleTag = 1

						// Update my timestamp to be this tag
						r.bookkeeping[seq].maxTime = ackWrite.OtherTag.Timestamp
					}
				}
			}

			if r.bookkeeping[seq].isAsyncWrite == 0 {
				// Check if I have received responses from a quorum
				if r.bookkeeping[seq].ackWrites >= r.writeQuorum && !r.bookkeeping[seq].doneFirstWait && !r.bookkeeping[seq].complete {
					r.bookkeeping[seq].doneFirstWait = true

					if r.bookkeeping[seq].staleTag == 0 { // All staleTag = FALSE
						// Reply to writer client
						dlog.Printf("GUS: reply to client %d +++ Fast Path +++\n", r.currentSeq)
						if r.bookkeeping[seq].proposal != nil {
							propreply := &genericsmrproto.ProposeReplyTS{
								TRUE,
								r.bookkeeping[seq].proposal.CommandId,
								state.NIL,
								r.bookkeeping[seq].proposal.Timestamp}
							r.ReplyProposeTS(propreply, r.bookkeeping[seq].proposal.Reply)
							r.bookkeeping[seq].complete = true
						}
						r.bcastUpdateView(seq, ackWrite.WriterID, r.currentTag[key].Timestamp)
						r.initializeView(key, r.currentTag[key])
						r.view[key][r.currentTag[key]][r.Id] = true
						r.storage[key][r.currentTag[key]] = r.bookkeeping[seq].valueToWrite

						// transform write into byte array
						if r.Durable {
							var b [24]byte
							binary.LittleEndian.PutUint64(b[0:8], uint64(key))
							binary.LittleEndian.PutUint32(b[8:12], uint32(r.currentTag[key].Timestamp))
							binary.LittleEndian.PutUint32(b[12:16], uint32(r.currentTag[key].WriterID))
							binary.LittleEndian.PutUint64(b[16:24], uint64(r.bookkeeping[seq].valueToWrite))
							r.StableStore.Write(b[:])
							r.sync()
						}
					} else {
						// There is a staleTag = TRUE
						r.currentTag[key] = gusproto.Tag{r.bookkeeping[seq].maxTime + 1, r.Id}
						r.bcastCommitWrite(seq, ackWrite.WriterID, r.currentTag[key].Timestamp, key, r.bookkeeping[seq].isAsyncWrite)
						// Make sure to receive AckCommit from a quorum
						r.bookkeeping[seq].waitForAckCommit = true
					}
				}
			} else {
				if r.bookkeeping[seq].ackWrites >= r.writeQuorum && !r.bookkeeping[seq].doneFirstWait && !r.bookkeeping[seq].complete {
					r.bookkeeping[seq].doneFirstWait = true
					if r.bookkeeping[seq].staleTag == 0 { // All staleTag = FALSE
						// Reply to writer client
						dlog.Printf("GUS: reply to client %d +++ Fast Path +++\n", r.currentSeq)
						if r.bookkeeping[seq].proposal != nil {
							propreply := &genericsmrproto.ProposeReplyTS{
								TRUE,
								r.bookkeeping[seq].proposal.CommandId,
								state.NIL,
								r.bookkeeping[seq].proposal.Timestamp}
							r.ReplyProposeTS(propreply, r.bookkeeping[seq].proposal.Reply)
							r.bookkeeping[seq].complete = true
						}
						r.asyncStorage = append(r.asyncStorage, &AsyncObj{key, seq, r.currentTag[key], r.bookkeeping[seq].valueToWrite})
					} else {
						// There is a staleTag = TRUE
						r.bcastCommitWrite(seq, ackWrite.WriterID, r.bookkeeping[seq].maxTime+1, key, r.bookkeeping[seq].isAsyncWrite)
						// Make sure to receive AckCommit from a quorum
						r.bookkeeping[seq].waitForAckCommit = true
					}
				}
			}
			break

		case commitWriteS := <-r.commitWriteChan:
			commitWrite := commitWriteS.(*gusproto.CommitWrite)
			commitTag := gusproto.Tag{commitWrite.CurrentTime, commitWrite.WriterID}
			seq := commitWrite.Seq
			key := commitWrite.Key
			isAsyncWrite := commitWrite.IsAsync

			if isAsyncWrite == 0 {
				if commitTag.GreaterThan(r.currentTag[key]) {
					r.bcastUpdateView(commitWrite.Seq, commitWrite.WriterID, commitWrite.CurrentTime)
					r.currentTag[key] = gusproto.Tag{commitTag.Timestamp, commitTag.WriterID}
				}

				_, existence := r.storage[key]
				if !existence {
					r.storage[key] = make(map[gusproto.Tag]state.Value)
				}
				// Move value from tmpStorage to Storage
				r.storage[key][commitTag] = r.tmpStorage[key][r.currentTag[key]]

				// transform write into byte array
				if r.Durable {
					var b [24]byte
					binary.LittleEndian.PutUint64(b[0:8], uint64(key))
					binary.LittleEndian.PutUint32(b[8:12], uint32(commitTag.Timestamp))
					binary.LittleEndian.PutUint32(b[12:16], uint32(commitTag.WriterID))
					binary.LittleEndian.PutUint64(b[16:24], uint64(r.tmpStorage[key][r.currentTag[key]]))
					r.StableStore.Write(b[:])
					r.sync()
				}

				delete(r.tmpStorage[key], commitTag)
				r.initializeView(key, commitTag)
				r.view[key][commitTag][r.Id] = true
			} else {
				//Find stuff from tmpAsyncStorage
				for i, obj := range r.tmpAsyncStorage {
					if obj.seq == seq && obj.tag.WriterID == commitWrite.WriterID {
						value := obj.value
						r.asyncStorage = append(r.asyncStorage, &AsyncObj{key, seq, commitTag, value})
						// remove this object from tmpAsyncStorage
						r.tmpAsyncStorage[i] = r.tmpAsyncStorage[len(r.tmpAsyncStorage)-1]
						r.tmpAsyncStorage = r.tmpAsyncStorage[:len(r.tmpAsyncStorage)-1]
						break
					}
				}
			}
			r.bcastAckCommit(commitWrite.Seq, commitWrite.WriterID)
			break

		case ackCommitS := <-r.ackCommitChan:
			ackCommit := ackCommitS.(*gusproto.AckCommit)
			seq := ackCommit.Seq
			r.bookkeeping[seq].ackCommits++
			key := r.bookkeeping[seq].key

			if r.bookkeeping[seq].isAsyncWrite == 0 {
				if r.bookkeeping[seq].waitForAckCommit && !r.bookkeeping[seq].complete && r.bookkeeping[seq].ackCommits >= r.writeQuorum {
					// Reply to client
					dlog.Printf("GUS: reply to client %d +++ Slow Path +++\n", r.currentSeq)
					if r.bookkeeping[seq].proposal != nil {
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							r.bookkeeping[seq].proposal.CommandId,
							state.NIL,
							r.bookkeeping[seq].proposal.Timestamp}
						r.ReplyProposeTS(propreply, r.bookkeeping[seq].proposal.Reply)
					}
					r.bookkeeping[seq].complete = true
					r.bcastUpdateView(seq, ackCommit.WriterID, r.currentTag[key].Timestamp)
					r.storage[key][r.currentTag[key]] = r.bookkeeping[seq].valueToWrite

					// transform write into byte array
					if r.Durable {
						var b [24]byte
						binary.LittleEndian.PutUint64(b[0:8], uint64(key))
						binary.LittleEndian.PutUint32(b[8:12], uint32(r.currentTag[key].Timestamp))
						binary.LittleEndian.PutUint32(b[12:16], uint32(r.currentTag[key].WriterID))
						binary.LittleEndian.PutUint64(b[16:24], uint64(r.bookkeeping[seq].valueToWrite))
						r.StableStore.Write(b[:])
						r.sync()
					}

					r.initializeView(key, r.currentTag[key])
					r.view[key][r.currentTag[key]][r.Id] = true
				}
			} else {
				if r.bookkeeping[seq].proposal != nil {
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						r.bookkeeping[seq].proposal.CommandId,
						state.NIL,
						r.bookkeeping[seq].proposal.Timestamp}
					r.ReplyProposeTS(propreply, r.bookkeeping[seq].proposal.Reply)
				}
				r.bookkeeping[seq].complete = true
				r.asyncStorage = append(r.asyncStorage, &AsyncObj{key, seq, r.currentTag[key], r.bookkeeping[seq].valueToWrite})
			}
			break

		case updateViewS := <-r.updateViewChan:
			updateView := updateViewS.(*gusproto.UpdateView)
			dlog.Printf("GUS: Replica %d: updating view from %d\n", r.Id, updateView.Sender)
			key := updateView.Key
			//seq := updateView.Seq

			r.initializeView(key, r.currentTag[key])
			r.view[key][r.currentTag[key]][updateView.Sender] = true

			// This piece of code is for n=5
			//if r.busyKey[key] {
			//	// If I'm still waiting to complete a read operation, only needed for n=5
			//	if r.bookkeeping[seq].proposal != nil && !r.bookkeeping[seq].complete {
			//		if (r.bookkeeping[seq].proposal.Command.Op == state.GET) && r.bookkeeping[seq].checkStorageForRead {
			//
			//			r.bookkeeping[seq].complete = true
			//			tag := gusproto.Tag{r.currentTag[key].Timestamp, r.currentTag[key].WriterID}
			//			// Reply to client
			//			propreply := &genericsmrproto.ProposeReplyTS{
			//				TRUE,
			//				r.bookkeeping[seq].proposal.CommandId,
			//				r.storage[key][tag],
			//				r.bookkeeping[seq].proposal.Timestamp}
			//			r.ReplyProposeTS(propreply, r.bookkeeping[seq].proposal.Reply)
			//
			//			// This line below simplifies the logic of reset()
			//			r.bookkeeping[seq].valueToWrite = r.storage[key][tag]
			//			r.busyKey[key] = false
			//			r.reset(seq)
			//		}
			//
			//	}
			//}
			break

		case readS := <-r.readChan:
			read := readS.(*gusproto.Read)
			r.bcastAckRead(read.Seq, read.ReaderID, read.Command.K)
			break

		case ackReadS := <-r.ackReadChan:
			ackRead := ackReadS.(*gusproto.AckRead)
			seq := ackRead.Seq
			key := r.bookkeeping[seq].key

			r.bookkeeping[seq].ackReads++
			currentTag := r.currentTag[key]
			// I receive a larger tag
			if currentTag.LessThan(ackRead.CurrentTag) {
				// Update key
				r.currentTag[key] = gusproto.Tag{ackRead.CurrentTag.Timestamp, ackRead.CurrentTag.WriterID}

				// Optimizing read for n=3
				//_, existence := r.storage[key]
				//if !existence {
				//	r.storage[key] = make(map[gusproto.Tag]state.Value)
				//}
				//r.storage[key][ackRead.CurrentTag] = ackRead.Value

				// This is not needed, because this replica can write to disk when receiving the write request
				//// transform write into byte array
				//if r.Durable {
				//	var b [24]byte
				//	binary.LittleEndian.PutUint64(b[0:8], uint64(key))
				//	binary.LittleEndian.PutUint32(b[8:12], uint32(ackRead.CurrentTag.Timestamp))
				//	binary.LittleEndian.PutUint32(b[12:16], uint32(ackRead.CurrentTag.WriterID))
				//	binary.LittleEndian.PutUint64(b[16:24], uint64(ackRead.Value))
				//	r.StableStore.Write(b[:])
				//	r.sync()
				//}

				r.initializeView(key, ackRead.CurrentTag)
				r.view[key][ackRead.CurrentTag][r.Id] = true
				r.bcastUpdateView(0, ackRead.CurrentTag.WriterID, ackRead.CurrentTag.Timestamp)
			}

			if r.bookkeeping[seq].ackReads >= r.readQurum && r.bookkeeping[seq].waitForAckRead {
				r.initializeView(key, r.currentTag[key])
				// Check if a quorum of replicas have received this value/tag
				if len(r.view[key][r.currentTag[key]]) >= r.readQurum {
					tag := gusproto.Tag{r.currentTag[key].Timestamp, r.currentTag[key].WriterID}
					// Reply to reader client
					propreply := &genericsmrproto.ProposeReplyTS{
						TRUE,
						r.bookkeeping[seq].proposal.CommandId,
						r.storage[key][tag],
						r.bookkeeping[seq].proposal.Timestamp}
					r.ReplyProposeTS(propreply, r.bookkeeping[seq].proposal.Reply)
					r.bookkeeping[seq].complete = true
					// This line below simplifies the logic of reset()
					r.bookkeeping[seq].valueToWrite = r.storage[key][tag]
					r.activeRead[key] = false
					r.reset(seq)
				} else {
					// Make sure the second phase waits for a quorum
					r.bookkeeping[seq].checkStorageForRead = true
				}
				r.bookkeeping[seq].waitForAckRead = false
			}
			break
		}
	}
}

func (r *Replica) initializeView(key state.Key, tag gusproto.Tag) {
	_, existence := r.view[key]
	if !existence {
		r.view[key] = make(map[gusproto.Tag][]bool)
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

// seq is associated with the operation that just completes
func (r *Replica) reset(seq int32) {
	// Optimization: process pending operations
	if len(r.pendingReads) != 0 {
		fmt.Print("Handling parallel read operations %d\n", len(r.pendingReads))
		var proposal *genericsmr.Propose
		for i := 0; i < len(r.pendingReads); i++ {
			proposal = r.pendingReads[i]

			propreply := &genericsmrproto.ProposeReplyTS{
				TRUE,
				proposal.CommandId,
				r.bookkeeping[seq].valueToWrite,
				proposal.Timestamp}
			r.ReplyProposeTS(propreply, proposal.Reply)
			//TODO: add this proposal to the booking for debugging/logging purpose
		}
		r.pendingReads = []*genericsmr.Propose{}
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
	ackReadMSG.Value = r.storage[key][r.currentTag[key]]
	args := &ackReadMSG

	r.bcastAll(r.ackReadRPC, args)
}

var writeMSG gusproto.Write

func (r *Replica) bcastWrite(seq int32, command state.Command, isAsync uint8) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	writeMSG.Seq = seq
	writeMSG.WriterID = r.Id
	writeMSG.CurrentTime = r.currentTag[r.bookkeeping[seq].key].Timestamp
	writeMSG.Command = command
	writeMSG.IsAsync = isAsync
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

func (r *Replica) bcastCommitWrite(seq int32, writerID int32, timestamp int32, key state.Key, isAsync uint8) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Write bcast failed:", err)
		}
	}()

	commitWriteMSG.Seq = seq
	commitWriteMSG.WriterID = writerID
	commitWriteMSG.CurrentTime = timestamp
	commitWriteMSG.Key = key
	commitWriteMSG.IsAsync = isAsync

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
