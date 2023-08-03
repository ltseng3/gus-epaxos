package paxosproto

import (
	"bufio"
	"encoding/binary"
	"gus-epaxos/src/fastrpc"
	"gus-epaxos/src/state"
	"io"
	"sync"
)

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *Read) New() fastrpc.Serializable {
	return new(Read)
}
func (t *Read) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type ReadCache struct {
	mu    sync.Mutex
	cache []*Read
}

func NewReadCache() *ReadCache {
	c := &ReadCache{}
	c.cache = make([]*Read, 0)
	return c
}

func (p *ReadCache) Get() *Read {
	var t *Read
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Read{}
	}
	return t
}
func (p *ReadCache) Put(t *Read) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Read) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.RequestorId
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	tmp32 = t.ReadId
	bs[4] = byte(tmp32 >> 24)
	bs[5] = byte(tmp32 >> 16)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32)
	wire.Write(bs)
}

func (t *Read) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.RequestorId = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.ReadId = int32(((uint32(bs[4]) << 24) | (uint32(bs[5]) << 16) | (uint32(bs[6]) << 8) | uint32(bs[7])))
	return nil
}

func (t *ReadReply) New() fastrpc.Serializable {
	return new(ReadReply)
}
func (t *ReadReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ReadReplyCache struct {
	mu    sync.Mutex
	cache []*ReadReply
}

func NewReadReplyCache() *ReadReplyCache {
	c := &ReadReplyCache{}
	c.cache = make([]*ReadReply, 0)
	return c
}

func (p *ReadReplyCache) Get() *ReadReply {
	var t *ReadReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ReadReply{}
	}
	return t
}
func (p *ReadReplyCache) Put(t *ReadReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ReadReply) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Instance
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	tmp32 = t.ReadId
	bs[4] = byte(tmp32 >> 24)
	bs[5] = byte(tmp32 >> 16)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *ReadReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Instance = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.ReadId = int32(((uint32(bs[4]) << 24) | (uint32(bs[5]) << 16) | (uint32(bs[6]) << 8) | uint32(bs[7])))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *Prepare) New() fastrpc.Serializable {
	return new(Prepare)
}
func (t *Prepare) BinarySize() (nbytes int, sizeKnown bool) {
	return 13, true
}

type PrepareCache struct {
	mu    sync.Mutex
	cache []*Prepare
}

func NewPrepareCache() *PrepareCache {
	c := &PrepareCache{}
	c.cache = make([]*Prepare, 0)
	return c
}

func (p *PrepareCache) Get() *Prepare {
	var t *Prepare
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Prepare{}
	}
	return t
}
func (p *PrepareCache) Put(t *Prepare) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Prepare) Marshal(wire io.Writer) {
	var b [13]byte
	var bs []byte
	bs = b[:13]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	tmp32 = t.Instance
	bs[4] = byte(tmp32 >> 24)
	bs[5] = byte(tmp32 >> 16)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32 >> 24)
	bs[9] = byte(tmp32 >> 16)
	bs[10] = byte(tmp32 >> 8)
	bs[11] = byte(tmp32)
	bs[12] = byte(t.ToInfinity)
	wire.Write(bs)
}

func (t *Prepare) Unmarshal(wire io.Reader) error {
	var b [13]byte
	var bs []byte
	bs = b[:13]
	if _, err := io.ReadAtLeast(wire, bs, 13); err != nil {
		return err
	}
	t.LeaderId = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.Instance = int32(((uint32(bs[4]) << 24) | (uint32(bs[5]) << 16) | (uint32(bs[6]) << 8) | uint32(bs[7])))
	t.Ballot = int32(((uint32(bs[8]) << 24) | (uint32(bs[9]) << 16) | (uint32(bs[10]) << 8) | uint32(bs[11])))
	t.ToInfinity = uint8(bs[12])
	return nil
}

func (t *PrepareReply) New() fastrpc.Serializable {
	return new(PrepareReply)
}
func (t *PrepareReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type PrepareReplyCache struct {
	mu    sync.Mutex
	cache []*PrepareReply
}

func NewPrepareReplyCache() *PrepareReplyCache {
	c := &PrepareReplyCache{}
	c.cache = make([]*PrepareReply, 0)
	return c
}

func (p *PrepareReplyCache) Get() *PrepareReply {
	var t *PrepareReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PrepareReply{}
	}
	return t
}
func (p *PrepareReplyCache) Put(t *PrepareReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PrepareReply) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.Instance
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	bs[4] = byte(t.OK)
	tmp32 = t.Ballot
	bs[5] = byte(tmp32 >> 24)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 8)
	bs[8] = byte(tmp32)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *PrepareReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.Instance = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.OK = uint8(bs[4])
	t.Ballot = int32(((uint32(bs[5]) << 24) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 8) | uint32(bs[8])))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *Accept) New() fastrpc.Serializable {
	return new(Accept)
}
func (t *Accept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type AcceptCache struct {
	mu    sync.Mutex
	cache []*Accept
}

func NewAcceptCache() *AcceptCache {
	c := &AcceptCache{}
	c.cache = make([]*Accept, 0)
	return c
}

func (p *AcceptCache) Get() *Accept {
	var t *Accept
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Accept{}
	}
	return t
}
func (p *AcceptCache) Put(t *Accept) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Accept) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	tmp32 = t.Instance
	bs[4] = byte(tmp32 >> 24)
	bs[5] = byte(tmp32 >> 16)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32 >> 24)
	bs[9] = byte(tmp32 >> 16)
	bs[10] = byte(tmp32 >> 8)
	bs[11] = byte(tmp32)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *Accept) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.LeaderId = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.Instance = int32(((uint32(bs[4]) << 24) | (uint32(bs[5]) << 16) | (uint32(bs[6]) << 8) | uint32(bs[7])))
	t.Ballot = int32(((uint32(bs[8]) << 24) | (uint32(bs[9]) << 16) | (uint32(bs[10]) << 8) | uint32(bs[11])))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *AcceptReply) New() fastrpc.Serializable {
	return new(AcceptReply)
}
func (t *AcceptReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 9, true
}

type AcceptReplyCache struct {
	mu    sync.Mutex
	cache []*AcceptReply
}

func NewAcceptReplyCache() *AcceptReplyCache {
	c := &AcceptReplyCache{}
	c.cache = make([]*AcceptReply, 0)
	return c
}

func (p *AcceptReplyCache) Get() *AcceptReply {
	var t *AcceptReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AcceptReply{}
	}
	return t
}
func (p *AcceptReplyCache) Put(t *AcceptReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *AcceptReply) Marshal(wire io.Writer) {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	tmp32 := t.Instance
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	bs[4] = byte(t.OK)
	tmp32 = t.Ballot
	bs[5] = byte(tmp32 >> 24)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 8)
	bs[8] = byte(tmp32)
	wire.Write(bs)
}

func (t *AcceptReply) Unmarshal(wire io.Reader) error {
	var b [9]byte
	var bs []byte
	bs = b[:9]
	if _, err := io.ReadAtLeast(wire, bs, 9); err != nil {
		return err
	}
	t.Instance = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.OK = uint8(bs[4])
	t.Ballot = int32(((uint32(bs[5]) << 24) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 8) | uint32(bs[8])))
	return nil
}

func (t *Commit) New() fastrpc.Serializable {
	return new(Commit)
}
func (t *Commit) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type CommitCache struct {
	mu    sync.Mutex
	cache []*Commit
}

func NewCommitCache() *CommitCache {
	c := &CommitCache{}
	c.cache = make([]*Commit, 0)
	return c
}

func (p *CommitCache) Get() *Commit {
	var t *Commit
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Commit{}
	}
	return t
}
func (p *CommitCache) Put(t *Commit) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Commit) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	tmp32 = t.Instance
	bs[4] = byte(tmp32 >> 24)
	bs[5] = byte(tmp32 >> 16)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32)
	tmp32 = t.Ballot
	bs[8] = byte(tmp32 >> 24)
	bs[9] = byte(tmp32 >> 16)
	bs[10] = byte(tmp32 >> 8)
	bs[11] = byte(tmp32)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Command))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Marshal(wire)
	}
}

func (t *Commit) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.LeaderId = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.Instance = int32(((uint32(bs[4]) << 24) | (uint32(bs[5]) << 16) | (uint32(bs[6]) << 8) | uint32(bs[7])))
	t.Ballot = int32(((uint32(bs[8]) << 24) | (uint32(bs[9]) << 16) | (uint32(bs[10]) << 8) | uint32(bs[11])))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Command = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Command[i].Unmarshal(wire)
	}
	return nil
}

func (t *CommitShort) New() fastrpc.Serializable {
	return new(CommitShort)
}
func (t *CommitShort) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type CommitShortCache struct {
	mu    sync.Mutex
	cache []*CommitShort
}

func NewCommitShortCache() *CommitShortCache {
	c := &CommitShortCache{}
	c.cache = make([]*CommitShort, 0)
	return c
}

func (p *CommitShortCache) Get() *CommitShort {
	var t *CommitShort
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommitShort{}
	}
	return t
}
func (p *CommitShortCache) Put(t *CommitShort) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommitShort) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.LeaderId
	bs[0] = byte(tmp32 >> 24)
	bs[1] = byte(tmp32 >> 16)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32)
	tmp32 = t.Instance
	bs[4] = byte(tmp32 >> 24)
	bs[5] = byte(tmp32 >> 16)
	bs[6] = byte(tmp32 >> 8)
	bs[7] = byte(tmp32)
	tmp32 = t.Count
	bs[8] = byte(tmp32 >> 24)
	bs[9] = byte(tmp32 >> 16)
	bs[10] = byte(tmp32 >> 8)
	bs[11] = byte(tmp32)
	tmp32 = t.Ballot
	bs[12] = byte(tmp32 >> 24)
	bs[13] = byte(tmp32 >> 16)
	bs[14] = byte(tmp32 >> 8)
	bs[15] = byte(tmp32)
	wire.Write(bs)
}

func (t *CommitShort) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.LeaderId = int32(((uint32(bs[0]) << 24) | (uint32(bs[1]) << 16) | (uint32(bs[2]) << 8) | uint32(bs[3])))
	t.Instance = int32(((uint32(bs[4]) << 24) | (uint32(bs[5]) << 16) | (uint32(bs[6]) << 8) | uint32(bs[7])))
	t.Count = int32(((uint32(bs[8]) << 24) | (uint32(bs[9]) << 16) | (uint32(bs[10]) << 8) | uint32(bs[11])))
	t.Ballot = int32(((uint32(bs[12]) << 24) | (uint32(bs[13]) << 16) | (uint32(bs[14]) << 8) | uint32(bs[15])))
	return nil
}
