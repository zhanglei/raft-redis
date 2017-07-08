package store

import (
	"sync"
)

type Set struct {
	sync.Mutex
	Key   string
	set   map[string]struct{}
}

func (s *Set) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.set)
}

func (s *Set) Add(key string) int  {
	s.Lock()
	defer  s.Unlock()

	if _,found := s.set[key];!found  {
		s.set[key] = struct{}{}
	}else{
		return 0
	}
	return 1
}

func (s *Set)Del(key string) error  {
	s.Lock()
	defer  s.Unlock()
	delete(s.set,key)
	return nil
}

func (s *Set)Members() *[][]byte {
	s.Lock()
	defer  s.Unlock()
	var ret [][]byte

	for key,_:=range s.set {
		ret = append(ret,[]byte(key))
	}
	return &ret
}

func (s *Set) Exists(key string) int {
	s.Lock()
	defer  s.Unlock()

	if _,found:= s.set[key] ; found {
		return 1
	}
	return 0
}

func NewSet(key string) *Set {
	return &Set{
		Key:   key,
		set: make(map[string]struct{}),
	}
}