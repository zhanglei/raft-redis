package redis

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"sync"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/pkg/fileutil"
	"os"
	"github.com/prometheus/common/log"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/raft"
	"encoding/json"
	"encoding/gob"
	"bytes"
)

type (
	HashValue   map[string][]byte
	HashHash    map[string]HashValue
	HashSub     map[string][]*ChannelWriter
	HashBrStack map[string]*Stack
	HashSet     map[string]*Set
)

type Database struct {
	values  HashValue
	hvalues HashHash
	brstack HashBrStack
	set HashSet
}

func NewDatabase(parent *Database) *Database {
	db := &Database{
		values:   make(HashValue),
		brstack:  make(HashBrStack),
		set    :  make(HashSet),
	}
	//db.children[0] = db
	return db
}

type DefaultHandler struct {
	*Database
	currentDb int
//	dbs       map[int]*Database

	wal          *wal.WAL
	snapshotter  *snap.Snapshotter

	c *Config
	raftStorage *raft.MemoryStorage
	rwmu sync.RWMutex

	lastIndex   uint64
	commitC     chan<- *string
}
type Op struct {
	Method string
	Args [][]byte
}

//list operation
func (h *DefaultHandler) Rpush(key string, value []byte, values ...[]byte) (int, error) {

	values = append([][]byte{value}, values...)
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	for _, value := range values {
		h.brstack[key].PushBack(value)
	}
	return h.brstack[key].Len(), nil
}

func (h *DefaultHandler) Brpop(key string, keys ...string) (data [][]byte, err error) {
	keys = append([]string{key}, keys...)
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

	if len(keys) == 0 {
		return nil, ErrParseTimeout
	}

	timeout, err := strconv.Atoi(keys[len(keys)-1])
	if err != nil {
		return nil, ErrParseTimeout
	}
	keys = keys[:len(keys)-1]

	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(time.Duration(timeout) * time.Second)
	} else {
		timeoutChan = make(chan time.Time)
	}

	finishedChan := make(chan struct{})

	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	go func() {
		defer close(finishedChan)
		selectCases := []reflect.SelectCase{}
		for _, k := range keys {
			key := string(k)
			if _, exists := h.brstack[key]; !exists {
				h.brstack[key] = NewStack(k)
			}
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(h.brstack[key].Chan),
			})
		}
		_, recv, _ := reflect.Select(selectCases)
		s, ok := recv.Interface().(*Stack)
		if !ok {
			err = fmt.Errorf("Impossible to retrieve data. Wrong type.")
			return
		}
		data = [][]byte{[]byte(s.Key), s.PopBack()}
	}()

	select {
	case <-finishedChan:
		return data, err
	case <-timeoutChan:
		return nil, nil
	}
	return nil, nil
}

func (h *DefaultHandler) Lrange(key string, start, stop int) ([][]byte, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

	h.rwmu.RLock()
	defer h.rwmu.RUnlock()

	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}

	if start < 0 {
		if start = h.brstack[key].Len() + start; start < 0 {
			start = 0
		}
	}

	var ret [][]byte
	if stop < 0 {
		stop =  h.brstack[key].Len() + stop
		if stop <0 {
			return nil,nil
		}
	}

	for i := start; i <= stop; i++ {
		if val := h.brstack[key].GetIndex(i); val != nil {
			ret = append(ret, val)
		}
	}
	return ret, nil
}

func (h *DefaultHandler) Lindex(key string, index int) ([]byte, error) {
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	return h.brstack[key].GetIndex(index), nil
}

func (h *DefaultHandler) Lpush(key string, value []byte, values ...[]byte) (int, error) {
	values = append([][]byte{value}, values...)

	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	for _, value := range values {
		h.brstack[key].PushFront(value)
	}
	return h.brstack[key].Len(), nil
}

func (h *DefaultHandler) Blpop(key string, keys ...string) (data [][]byte, err error) {
	keys = append([]string{key}, keys...)
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

	if len(keys) == 0 {
		return nil, ErrParseTimeout
	}

	timeout, err := strconv.Atoi(keys[len(keys)-1])
	if err != nil {
		return nil, ErrParseTimeout
	}
	keys = keys[:len(keys)-1]

	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(time.Duration(timeout) * time.Second)
	} else {
		timeoutChan = make(chan time.Time)
	}

	finishedChan := make(chan struct{})

	go func() {
		defer close(finishedChan)
		selectCases := []reflect.SelectCase{}
		for _, k := range keys {
			key := string(k)
			if _, exists := h.brstack[key]; !exists {
				h.brstack[key] = NewStack(k)
			}
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(h.brstack[key].Chan),
			})
		}
		_, recv, _ := reflect.Select(selectCases)
		s, ok := recv.Interface().(*Stack)
		if !ok {
			err = fmt.Errorf("Impossible to retrieve data. Wrong type.")
			return
		}
		data = [][]byte{[]byte(s.Key), s.PopFront()}
	}()

	select {
	case <-finishedChan:
		return data, err
	case <-timeoutChan:
		return nil, nil
	}
	return nil, nil
}

func (h *DefaultHandler)Lpop(key string) ([]byte,error) {

	if h.Database == nil || h.brstack == nil{
		return nil, nil
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if _,found := h.brstack[key];!found{
		return nil,nil
	}

	return h.brstack[key].PopFront(),nil
}

func (h *DefaultHandler)Rpop(key string) ([]byte,error) {

	if h.Database == nil || h.brstack == nil{
		return nil, nil
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if _,found := h.brstack[key];!found{
		return nil,nil
	}

	return h.brstack[key].PopBack(),nil
}

//set operation
func (h *DefaultHandler) Sadd (key string, values ...string) (int ,error){
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

	if _, exists := h.set[key]; !exists {
		h.set[key] = NewSet(key)
	}

	count := 0
	for _,value :=range values {
		count =count + h.set[key].Add(value)
	}
	return count,nil
}


func (h *DefaultHandler) Scard (key string)( int,error) {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.set[key]; !exists {
		return 0,nil
	}
	return h.set[key].Len(),nil
}


func (h *DefaultHandler) Smembers (key string)  ([][]byte,error) {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.set[key]; !exists {
		return nil,nil
	}

	return *h.set[key].Members(),nil
}

//hash set
func (h *DefaultHandler) Hget(key, subkey string) ([]byte, error) {
	if h.Database == nil || h.hvalues == nil {
		return nil, nil
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if v, exists := h.hvalues[key]; exists {
		if v, exists := v[subkey]; exists {
			return v, nil
		}
	}
	return nil, nil
}

func (h *DefaultHandler) Hset(key, subkey string, value []byte) (int, error) {
	ret := 0
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}
	if _, exists := h.hvalues[key]; !exists {
		h.hvalues[key] = make(HashValue)
		ret = 1
	}

	if _, exists := h.hvalues[key][subkey]; !exists {
		ret = 1
	}

	h.hvalues[key][subkey] = value

	return ret, nil
}

func (h *DefaultHandler) Hgetall(key string) (HashValue, error) {
	if h.Database == nil || h.hvalues == nil {
		return nil, nil
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	return h.hvalues[key], nil
}

func (h *DefaultHandler) Get(key string) ([]byte, error) {
	if h.Database == nil || h.values == nil {
		return nil, nil
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	return h.values[key], nil
}

func (h *DefaultHandler) Set(key string, value []byte) error {
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = NewDatabase(nil)
	}

	h.values[key] = value
	return nil
}

func (h *DefaultHandler) Del(key string, keys ...string) (int, error) {

	keys = append([]string{key}, keys...)
	if h.Database == nil {
		return 0, nil
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	count := 0
	for _, k := range keys {
		if _, exists := h.values[k]; exists {
			delete(h.values, k)
			count++
		}
		if _, exists := h.hvalues[key]; exists {
			delete(h.hvalues, k)
			count++
		}

		if _, exists := h.set[key]; exists {
			delete(h.set, k)
			count++
		}
	}
	return count, nil
}

func (h *DefaultHandler) Ping() (*StatusReply, error) {
	return &StatusReply{code: "PONG"}, nil
}


/*func (h *DefaultHandler) Select(key string) error {
	if h.dbs == nil {
		h.dbs = map[int]*Database{0: h.Database}
	}
	index, err := strconv.Atoi(key)
	if err != nil {
		return err
	}
	h.dbs[h.currentDb] = h.Database
	h.currentDb = index
	if _, exists := h.dbs[index]; !exists {
		println("DB not exits, create ", index)
		h.dbs[index] = NewDatabase(nil)
	}
	h.Database = h.dbs[index]
	return nil
}*/

func (h *DefaultHandler) Monitor() (*MonitorReply, error) {
	return &MonitorReply{}, nil
}

func NewDefaultHandler( c *Config) *DefaultHandler {
	db := NewDatabase(nil)

	if !fileutil.Exist(c.snapDir) {
		if err := os.Mkdir(c.snapDir, 0750); err != nil {
			log.Fatal(" create snapshot dir err &s ",err.Error())
		}
	}


	commitC := make(chan *string)
	ret := &DefaultHandler{
		Database:  db,
		currentDb: 0,
		//dbs:       map[int]*Database{0: db},
		c : c,
		commitC: commitC,
	}
	go ret.readCommits(commitC)
	ret.snapshotter = snap.New(c.snapDir)
	ret.wal = ret.replayWAL()


	return ret
}


func (rc *DefaultHandler) replayWAL() *wal.WAL {
	snapshot := rc.loadSnapshot()


	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}

	rc.raftStorage.SetHardState(st)

	// append to storage so raftd starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
}


func (s *DefaultHandler) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.rwmu.Lock()
	//s.kvStore = store
	s.rwmu.Unlock()
	return nil
}



func (s *DefaultHandler) readCommits(commitC <-chan *string) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Fatal(err)
			}
			//log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Fatal(err)
			}
			continue
		}

		var op Op
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&op); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		//log.Printf("do commit %s %s",dataKv.Key,dataKv.Args)
		s.rwmu.Lock()

/*		switch dataKv.Opt {
		case "SET" :
			s.kvStore[dataKv.Key] = dataKv.Val
		case "DEL" :
			delete(s.kvStore,dataKv.Key)
		default:
			//do nothing
		}*/

		s.rwmu.Unlock()
	}
}

func (rc *DefaultHandler) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}


// openWAL returns a WAL ready for reading.
func (rc *DefaultHandler) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.c.walDir) {
		if err := os.Mkdir(rc.c.walDir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(rc.c.walDir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, err := wal.Open(rc.c.walDir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

func (h *DefaultHandler) getSnapshot()  ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	enc.Encode(*h.Database)
	return b.Bytes(),nil
}