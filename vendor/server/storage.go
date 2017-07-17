package server

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"github.com/coreos/etcd/snap"
)

var _Storage * Storage
// a key-value store backed by raftd
type Storage struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
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
	println(buf.String())
	s.proposeC <- string(buf.Bytes())
}

func (s *Storage) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {

		println("ddddd==================")
		if data == nil {

			println("recive nil")
			// done replaying log; new data incoming
			// OR signaled to load snapshot
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
			println(*data)
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		log.Printf("do commit %s %s",dataKv.Method,dataKv.Args)
		s.mu.Lock()

		switch dataKv.Method {
		case "set" :
			s.Redis.methodSet(dataKv.Args)
		case "del" :
			num := s.Redis.methodDel(dataKv.Args)
			if respchan,found :=Conns[dataKv.Conn];found {
				respchan <- num
			}
		case "hset":
			num := s.Redis.methodHset(dataKv.Args)
			if respchan,found :=Conns[dataKv.Conn];found {
				respchan <- num
			}

		case "rpush":
			num := s.Redis.methodRpush(dataKv.Args)
			if respchan,found :=Conns[dataKv.Conn];found {
				respchan <- num
			}
		case "lpush":
			num := s.Redis.methodLpush(dataKv.Args)
			if respchan,found :=Conns[dataKv.Conn];found {
				respchan <- num
			}
		case "lpop":
			byteArr := s.Redis.methodLpop(dataKv.Args)
			if respchan,found :=Conns[dataKv.Conn];found {
				respchan <- byteArr
			}
		case "rpop":
			byteArr := s.Redis.methodRpop(dataKv.Args)
			if respchan,found :=Conns[dataKv.Conn];found {
				respchan <- byteArr
			}
		case "sadd":
			num := s.Redis.methodSadd(dataKv.Args)
			if respchan,found :=Conns[dataKv.Conn];found {
				respchan <- num
			}

		default:
			//do nothing*/
		}
		s.mu.Unlock()
	}
	println("no running now")
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (h *Storage) GetSnapshot()  ([]byte, error) {
	var b bytes.Buffer
	h.mu.Lock()
	enc := gob.NewEncoder(&b)
	enc.Encode(*h.Redis)
	h.mu.Unlock()
	return b.Bytes(),nil
}

func (s *Storage) recoverFromSnapshot(snapshot []byte) error {
	var db Database
	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&db); err != nil {
		return err
	}
	s.mu.Lock()
	s.Redis = &db
	s.mu.Unlock()
	return nil
}
