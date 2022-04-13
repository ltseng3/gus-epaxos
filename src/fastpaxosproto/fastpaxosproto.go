package fastpaxosproto

import (
	"state"
)

// PreAccept
type Write struct {
	Seq      int32
	WriterID int32
	Version  int32
	Command  state.Command
}

// AckPreAccept/Response
type AckWrite struct {
	Seq           int32
	WriterID      int32
	LatestVersion int32
}

type CommitWrite struct {
	Seq     int32
	ID      int32
	Version int32
	Key     state.Key
	Value   state.Value
}

type AckCommit struct {
	Seq         int32
	Coordinator int32
}

// PreAccept
type Read struct {
	Seq      int32
	ReaderID int32
	Version  int32
	Command  state.Command
}

// AckPreAccept/Response
type AckRead struct {
	Seq      int32
	ReaderID int32
	Version  int32
	Value    state.Value
}

