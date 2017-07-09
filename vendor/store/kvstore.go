// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"

	"github.com/coreos/etcd/snap"
)

// a key-value store backed by raftd
type KvStore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	Redis       *Database
	snapshotter *snap.Snapshotter
	Conns *map[string]chan interface{}
}

type kv struct {
	Method string
	Args [][]byte
	Conn string
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error,Conns  *map[string]chan interface{}) *KvStore {
	s := &KvStore{proposeC: proposeC, Redis: NewDatabase(), snapshotter: snapshotter,Conns:Conns}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raftd into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}



func (s *KvStore) Propose(m string, a [][]byte,conn string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{m,a,conn}); err != nil {
		log.Fatal(err)
	}

	s.proposeC <- string(buf.Bytes())
}

func (s *KvStore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
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
			if respchan,found :=(*s.Conns)[dataKv.Conn];found {
				respchan <- num
			}
		default:
			//do nothing*/
		}

		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (h *KvStore) GetSnapshot()  ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	enc.Encode(*h.Redis)
	return b.Bytes(),nil
}

func (s *KvStore) recoverFromSnapshot(snapshot []byte) error {
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
