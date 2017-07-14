package store

import (
	"sync"
)

type Stack struct {
	mu sync.Mutex
	Key   string
	Stack [][]byte
	Chan  chan *Stack
}

func (s *Stack) PopBack() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Stack == nil || len(s.Stack) == 0 {
		return nil
	}

	var ret []byte
	if len(s.Stack)-1 == 0 {
		ret, s.Stack = s.Stack[0], [][]byte{}
	} else {
		ret, s.Stack = s.Stack[len(s.Stack)-1], s.Stack[:len(s.Stack)-1]
	}
	return ret
}

func (s *Stack) PushBack(val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Stack == nil {
		s.Stack = [][]byte{}
	}

	go func() {
		if s.Chan != nil {
			s.Chan <- s
		}
	}()
	s.Stack = append(s.Stack, val)
}

func (s *Stack) PopFront() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Stack == nil || len(s.Stack) == 0 {
		return nil
	}

	var ret []byte
	if len(s.Stack)-1 == 0 {
		ret, s.Stack = s.Stack[0], [][]byte{}
	} else {
		ret, s.Stack = s.Stack[0], s.Stack[1:]
	}
	return ret
}

func (s *Stack) PushFront(val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Stack == nil {
		s.Stack = [][]byte{}
	}

	s.Stack = append([][]byte{val}, s.Stack...)
	go func() {
		if s.Chan != nil {
			s.Chan <- s
		}
	}()
}

// GetIndex return the element at the requested index.
// If no element correspond, return nil.
func (s *Stack) GetIndex(index int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < 0 {
		if len(s.Stack)+index >= 0 {
			return s.Stack[len(s.Stack)+index]
		}
		return nil
	}
	if len(s.Stack) > index {
		return s.Stack[index]
	}
	return nil
}

func (s *Stack) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Stack)
}

func NewStack(key string) *Stack {
	return &Stack{
		Stack: [][]byte{},
		Chan:  make(chan *Stack),
		Key:   key,
	}
}
