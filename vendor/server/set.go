package server

import (
	"sync"
)

type Set struct {
	mu sync.Mutex
	Key   string
	Set   map[string]struct{}
}

func (s *Set) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Set)
}

func (s *Set) Add(key string) int  {
	s.mu.Lock()
	defer  s.mu.Unlock()
	if _,found := s.Set[key];!found  {
		s.Set[key] = struct{}{}
	}else{
		return 0
	}
	return 1
}

func (s *Set)Del(key string) error  {
	s.mu.Lock()
	defer  s.mu.Unlock()
	delete(s.Set,key)
	return nil
}

func (s *Set)Members() *[][]byte {
	s.mu.Lock()
	defer  s.mu.Unlock()
	var ret [][]byte

	for key,_:=range s.Set {
		ret = append(ret,[]byte(key))
	}
	return &ret
}

func (s *Set) Exists(key string) int {
	s.mu.Lock()
	defer  s.mu.Unlock()
	if _,found:= s.Set[key] ; found {
		return 1
	}
	return 0
}

func NewSet(key string) *Set {
	return &Set{
		Key:   key,
		Set: make(map[string]struct{}),
	}
}