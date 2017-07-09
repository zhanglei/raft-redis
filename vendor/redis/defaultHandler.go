package redis

import (
	"sync"
	"store"
	"errors"
)

type DefaultHandler struct {
	* store.Database
	currentDb int
	kv *store.KvStore
	c *store.Config
	rwmu sync.RWMutex
}
type Op struct {
	Method string
	Args [][]byte
}

//list operation
func (h *DefaultHandler) Rpush(r *Request,key string, value []byte, values ...[]byte) (int, error) {
	values = append([][]byte{value}, values...)
	if h.Database == nil {
		h.Database = store.NewDatabase()
	}
	h.kv.Propose("rpush",append([][]byte{[]byte(key)},values...),r.Conn)
	ret,ok := <- (*r.Conns)[r.Conn]
	if !ok {
		return 0,errors.New("rpush op something errors")
	}
	return ret.(int), nil
}


func (h *DefaultHandler) Lrange(key string, start, stop int) ([][]byte, error) {
	if h.Database == nil {
		h.Database = store.NewDatabase()
	}

	h.rwmu.RLock()
	defer h.rwmu.RUnlock()

	if _, exists := h.Brstack[key]; !exists {
		h.Brstack[key] = store.NewStack(key)
	}

	if start < 0 {
		if start = h.Brstack[key].Len() + start; start < 0 {
			start = 0
		}
	}

	var ret [][]byte
	if stop < 0 {
		stop =  h.Brstack[key].Len() + stop
		if stop <0 {
			return nil,nil
		}
	}

	for i := start; i <= stop; i++ {
		if val := h.Brstack[key].GetIndex(i); val != nil {
			ret = append(ret, val)
		}
	}
	return ret, nil
}

func (h *DefaultHandler) Lindex(key string, index int) ([]byte, error) {
	if h.Database == nil {
		h.Database = store.NewDatabase()
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if _, exists := h.Brstack[key]; !exists {
		h.Brstack[key] = store.NewStack(key)
	}
	return h.Brstack[key].GetIndex(index), nil
}

func (h *DefaultHandler) Lpush(r *Request,key string, value []byte, values ...[]byte) (int, error) {
	values = append([][]byte{value}, values...)

	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = store.NewDatabase()
	}
	h.kv.Propose("rpush",append([][]byte{[]byte(key)},values...),r.Conn)

	ret,ok := <- (*r.Conns)[r.Conn]
	if !ok {
		return 0,errors.New("rpush op something errors")
	}
	return ret.(int), nil
}


func (h *DefaultHandler)Lpop(r *Request,key string) ([]byte,error) {

	if h.Database == nil || h.Brstack == nil{
		return nil, nil
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()

	if _,found := h.Brstack[key];!found{
		return nil,nil
	}
	h.kv.Propose("lpop",append([][]byte{[]byte(key)}),r.Conn)
	ret,ok := <- (*r.Conns)[r.Conn]
	if !ok {
		return []byte{},errors.New("lpop op something errors")
	}
	return ret.([]byte),nil

	//return h.Brstack[key].PopFront(),nil
}

func (h *DefaultHandler)Rpop(r *Request,key string) ([]byte,error) {

	if h.Database == nil || h.Brstack == nil{
		return nil, nil
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if _,found := h.Brstack[key];!found{
		return nil,nil
	}
	h.kv.Propose("rpop",append([][]byte{[]byte(key)}),r.Conn)

	ret,ok := <- (*r.Conns)[r.Conn]
	if !ok {
		return []byte{},errors.New("rpop op something errors")
	}

	return ret.([]byte),nil
}

//set operation
func (h *DefaultHandler) Sadd (r *Request,key string, values ...string) (int ,error){
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = store.NewDatabase()
	}

	var bytes [][]byte
	for _,value :=range values {
		bytes = append(bytes, []byte(value))
	}
	h.kv.Propose("sadd",append([][]byte{[]byte(key)},bytes...),r.Conn)
	num,ok := <- (*r.Conns)[r.Conn]
	if !ok {
		return 0,errors.New("sadd op something errors")
	}
	return num.(int),nil
}


func (h *DefaultHandler) Scard (key string)( int,error) {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if h.Database == nil {
		h.Database = store.NewDatabase()
	}
	if _, exists := h.Hvset[key]; !exists {
		return 0,nil
	}
	return h.Hvset[key].Len(),nil
}


func (h *DefaultHandler) Smembers (key string)  ([][]byte,error) {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if h.Database == nil {
		h.Database = store.NewDatabase()
	}
	if _, exists := h.Hvset[key]; !exists {
		return nil,nil
	}

	return *h.Hvset[key].Members(),nil
}

//hash set
func (h *DefaultHandler) Hget(key, subkey string) ([]byte, error) {
	if h.Database == nil || h.Hvalues == nil {
		return nil, nil
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()

	if v, exists := h.Hvalues[key]; exists {
		if v, exists := v[subkey]; exists {
			return v, nil
		}
	}
	return nil, nil
}

func (h *DefaultHandler) Hset(r *Request,key, subkey string, value []byte) (int, error) {

	h.kv.Propose("hset",append([][]byte{[]byte(key)},[]byte(subkey),value),r.Conn)
	num,ok := <- (*r.Conns)[r.Conn]
	if !ok {
		return 0,errors.New("del op something errors")
	}
	return num.(int), nil
}

func (h *DefaultHandler) Hgetall(key string) (store.HashValue, error) {
	if h.Database == nil || h.Hvalues == nil {
		return nil, nil
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	return h.Hvalues[key], nil
}

func (h *DefaultHandler) Get(key string) ([]byte, error) {
	if h.Database == nil || h.Values == nil {
		return nil, nil
	}
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	return h.Values[key], nil
}

func (h *DefaultHandler) Set(key string, value []byte) error {
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	h.kv.Propose("set",append([][]byte{[]byte(key)},value),"")
	return nil
}

func (h *DefaultHandler) Del(r *Request,key string, keys ...string) (int, error) {
	keys = append([]string{key}, keys...)
	if h.Database == nil {
		return 0, nil
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	var bytes [][]byte
	for _, k := range keys {
		bytes = append(bytes,[]byte(k))
	}
	h.kv.Propose("del",bytes,r.Conn)
	num,ok := <- (*r.Conns)[r.Conn]
	if !ok {
		return 0,errors.New("del op something errors")
	}
	return num.(int), nil
}

func NewDefaultHandler( c *store.Config,kv *store.KvStore) *DefaultHandler {
	ret := &DefaultHandler{
		kv:kv,
		Database:  kv.Redis,
		currentDb: 0,
		c : c,
	}
	return ret
}
