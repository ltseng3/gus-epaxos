package paxosproto

import (
	"gus-epaxos/src/state"
)

const (
	PREPARE uint8 = iota
	PREPARE_REPLY
	ACCEPT
	ACCEPT_REPLY
	COMMIT
	COMMIT_SHORT
)

type Prepare struct {
	LeaderId   int32
	Instance   int32
	Ballot     int32
	ToInfinity uint8
}

type PrepareReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
	Command  []state.Command
}

type Accept struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
}

type AcceptReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Count    int32
	Ballot   int32
}

type RMWGet struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
}

type RMWGetReply struct {
	Instance int32
	Ballot   int32
	Key      int
	Payload  Payload
}

type RMWSet struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Key      int
	Payload  Payload
}

type RMWSetReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
}

type Read struct {
	RequesterId int32
	ReadId      int32
}

type ReadReply struct {
	Instance int32
	ReadId   int32
}
