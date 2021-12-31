package gusproto

import (
	"state"
)

const (
	PREPARE uint8 = iota
	PREPARE_REPLY
	ACCEPT
	ACCEPT_REPLY
	COMMIT
	COMMIT_SHORT
	WRITE
	ACKWRITE
	COMMITWRITE
)

type Tag struct {
	Timestamp int32
	WriterID  int32
}

// The following is from Gryff codebase
// https://github.com/matthelb/gryff/blob/master/src/abdproto/abdproto.go

func (a *Tag) Compare(b Tag) int {
	if a.Timestamp < b.Timestamp || a.Timestamp == b.Timestamp && a.WriterID < b.WriterID {
		return -1;
	} else if a.Timestamp == b.Timestamp && a.WriterID == b.WriterID {
		return 0;
	} else {
		return 1;
	}
}

func (a *Tag) GreaterThan(b Tag) bool {
	return a.Compare(b) > 0
}

func (a *Tag) LessThan(b Tag) bool {
	return a.Compare(b) < 0
}

func (a *Tag) Equals(b Tag) bool {
	return a.Compare(b) == 0
}



type Write struct {
	Seq         int32
	WriterID    int32
	CurrentTime int32
	Command     state.Command
}

type AckWrite struct {
	Seq 		int32
	WriterID	int32
	StaleTag	uint8
	OtherTag	Tag
}

type CommitWrite struct {
	Seq         int32
	Key         state.Key
	WriterID    int32
	CurrentTime int32
}

type AckCommit struct {
	Seq 		int32
	WriterID	int32
}

type UpdateView struct {
	Seq 		int32
	Key         state.Key
	WriterID	int32
	CurrentTime	int32
	Sender		int32
}

type Read struct {
	Seq      int32
	ReaderID int32
	Command  state.Command
}

type AckRead struct {
	Seq        int32
	ReaderID   int32
	CurrentTag Tag
	Value      state.Value
}

type Prepare struct {
	LeaderId   int32
	Instance   int32
	Ballot     int32
	ToInfinity uint8
	Test 	   int32
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
