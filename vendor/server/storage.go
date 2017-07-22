package server

import (
	"bytes"
	"encoding/gob"
	"log"
	"github.com/coreos/etcd/snap"
	"sync"
)

var _Storage * Storage
var _DBRwmu sync.RWMutex

// a key-value store backed by raftd
type Storage struct {
	proposeC    chan<- string // channel for proposing updates
	Redis       *Database
	snapshotter *snap.Snapshotter
}

type kv struct {
	Method string
	Args [][]byte
	Conn string
}

func Run() {
	_Storage.snapshotter = <-snapshotterReady
	// replay log into key-value map
	_Storage.readCommits(commitC, errorC)
	// read commits from raftd into kvStore map until error
	go _Storage.readCommits(commitC, errorC)
}



func (s *Storage) Propose(m string, a [][]byte,conn string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{m,a,conn}); err != nil {
		log.Fatal(err)
	}
	//println(buf.String())
	s.proposeC <- string(buf.Bytes())
}

func (s *Storage) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			//println(" readCommits recive nil")
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}else {
			if *data == "" {
				continue
			}
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		//log.Printf("do commit %s %s %s",dataKv.Method,dataKv.Args,dataKv.Conn)
		_DBRwmu.Lock()

		switch dataKv.Method {
		case "set" :
			s.Redis.methodSet(dataKv.Args)
		case "del" :
			num := s.Redis.methodDel(dataKv.Args)
			if Conns.Exists(dataKv.Conn){
				respchan := Conns.Get(dataKv.Conn)
				respchan <- num
			}
		case "hset":
			num := s.Redis.methodHset(dataKv.Args)
			if Conns.Exists(dataKv.Conn){
				respchan := Conns.Get(dataKv.Conn)
				respchan <- num
			}

		case "rpush":
			num := s.Redis.methodRpush(dataKv.Args)
			if Conns.Exists(dataKv.Conn){
				respchan := Conns.Get(dataKv.Conn)
				respchan <- num
			}
		case "lpush":
			num := s.Redis.methodLpush(dataKv.Args)
			if Conns.Exists(dataKv.Conn){
				respchan := Conns.Get(dataKv.Conn)
				respchan <- num
			}
		case "lpop":
			byteArr := s.Redis.methodLpop(dataKv.Args)
			if Conns.Exists(dataKv.Conn){
				respchan := Conns.Get(dataKv.Conn)
				respchan <- byteArr
			}
		case "rpop":
			byteArr := s.Redis.methodRpop(dataKv.Args)
			if Conns.Exists(dataKv.Conn){
				respchan := Conns.Get(dataKv.Conn)
				respchan <- byteArr
			}
		case "sadd":
			num := s.Redis.methodSadd(dataKv.Args)
			if Conns.Exists(dataKv.Conn){
				respchan := Conns.Get(dataKv.Conn)
				respchan <- num
			}

		default:
			//do nothing*/
		}
		_DBRwmu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *Storage) GetSnapshot()  ([]byte, error) {
	var b bytes.Buffer
	_DBRwmu.Lock()
	enc := gob.NewEncoder(&b)
	List  := &(*s.Redis).dlList
	s.Redis.HList = make(HashList)
	for key,v:= range *List {
		s.Redis.HList[key] = v.Values()
	}
	enc.Encode(*s.Redis)
	s.Redis.HList = nil
	_DBRwmu.Unlock()
	return b.Bytes(),nil
}

func (s *Storage) recoverFromSnapshot(snapshot []byte) error {
	var db Database
	_DBRwmu.Lock()
	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&db); err != nil {
		return err
	}
	db.dlList = make(HashBrStack)
	for key,v := range db.HList {
		if _,found := s.Redis.dlList[key];!found {
			db.dlList[key] = NewList()
		}
		db.dlList[key].Add(v...)
	}
	db.HList = nil
	s.Redis = &db
	_DBRwmu.Unlock()
	return nil
}
