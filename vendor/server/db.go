package server

import (
	"errors"
	"strconv"
	"github.com/coreos/etcd/raft/raftpb"
	//"sync"
	"time"
	"fmt"
)

type (
	HashValue map[string][]byte
	HashHash map[string]HashValue
	HashBrStack map[string]*Stack
	HashSet map[string]*Set
)

type Op struct {
	Method string
	Args   [][]byte
}

type Database struct {
	Values  HashValue
	Hvalues HashHash
	Brstack HashBrStack
	Hvset   HashSet
	//rwmu    sync.RWMutex  @todo add rw mutex
}

func NewDatabase() *Database {
	db := &Database{
		Values:  make(HashValue),
		Brstack: make(HashBrStack),
		Hvset:   make(HashSet),
		Hvalues: make(HashHash),
	}
	return db
}

func (d *Database) methodSet(b [][]byte) {
	d.Values[string(b[0])] = b[1]
}

func (d *Database) methodDel(b [][]byte) int {
	count := 0
	for _, k := range b {
		key := string(k)
		if _, exists := d.Values[key]; exists {
			delete(d.Values, key)
			count++
		}
		if _, exists := d.Values[key]; exists {
			delete(d.Values, key)
			count++
		}
		if _, exists := d.Hvset[key]; exists {
			delete(d.Hvset, key)
			count++
		}
	}
	return count
}

func (d *Database) methodHset(b [][]byte) int {
	ret := 0
	key := string(b[0])
	subkey := string(b[1])
	value := b[2]
	if _, exists := d.Hvalues[key]; !exists {
		d.Hvalues[key] = make(HashValue)
		ret = 1
	}
	if _, exists := d.Hvalues[key][subkey]; !exists {
		ret = 1
	}
	d.Hvalues[key][subkey] = value
	return ret
}

//func (d *Database)methodRpush(key string, value []byte, values ...[]byte) int {
func (d *Database) methodRpush(b [][]byte) int {
	key := string(b[0])
	values := b[1:]
	if _, exists := d.Brstack[key]; !exists {
		d.Brstack[key] = NewStack(key)
	}
	for _, value := range values {
		d.Brstack[key].PushBack(value)
	}
	return d.Brstack[key].Len()
}

//func (d *Database)methodLpush(key string, value []byte, values ...[]byte) int {
func (d *Database) methodLpush(b [][]byte) int {
	key := string(b[0])
	values := b[1:]
	if _, exists := d.Brstack[key]; !exists {
		d.Brstack[key] = NewStack(key)
	}
	for _, value := range values {
		d.Brstack[key].PushFront(value)
	}
	return d.Brstack[key].Len()
}

//func (d *Database)methodLpop(key string) []byte {
func (d *Database) methodLpop(b [][]byte) []byte {
	key := string(b[0])
	return d.Brstack[key].PopFront()
}

//func (d *Database)methodRpop(key string) []byte {
func (d *Database) methodRpop(b [][]byte) []byte {
	key := string(b[0])
	return d.Brstack[key].PopBack()
}

//func (d *Database)methodSadd(key string, values ...string) int {
func (d *Database) methodSadd(b [][]byte) int {
	count := 0
	key := string(b[0])
	values := b[1:]

	if _, exists := d.Hvset[key]; !exists {
		d.Hvset[key] = NewSet(key)
	}

	for _, value := range values {
		count = count + d.Hvset[key].Add(string(value))
	}
	return count
}

func (h *Database) AddNode(id string, url []byte) error {
	nodeId, err := strconv.ParseUint(id, 10, 0)
	if err != nil {
		return err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeId,
		Context: url,
	}
	confChangeC <- cc
	return nil
}

func (h *Database) RemoveNode(id string) error {
	nodeId, err := strconv.ParseUint(id, 10, 0)
	if err != nil {
		return err
	}
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeId,
	}
	confChangeC <- cc
	return nil
}

//list operation
func (h *Database) Rpush(r *Request, key string, value []byte, values ...[]byte) (int, error) {
	values = append([][]byte{value}, values...)
	k := fmt.Sprintf("%s%d",r.Conn,time.Now().UnixNano())
	Conns.Add(k,make(chan interface{}))
	defer  Conns.Del(k)
	_Storage.Propose("rpush", append([][]byte{[]byte(key)}, values...),k)
	ret, ok := <- Conns.Get(k)
	if !ok {
		return 0, errors.New("rpush op something errors")
	}
	close(Conns.Get(k))
	return ret.(int), nil
}

func (h *Database) Lrange(key string, start, stop int) ([][]byte, error) {
	if _, exists := h.Brstack[key]; !exists {
		h.Brstack[key] = NewStack(key)
	}

	if start < 0 {
		if start = h.Brstack[key].Len() + start; start < 0 {
			start = 0
		}
	}
	var ret [][]byte
	if stop < 0 {
		stop = h.Brstack[key].Len() + stop
		if stop < 0 {
			return nil, nil
		}
	}
	for i := start; i <= stop; i++ {
		if val := h.Brstack[key].GetIndex(i); val != nil {
			ret = append(ret, val)
		}
	}
	return ret, nil
}

func (h *Database) Llen(key string) (int, error) {
	if _, exists := h.Brstack[key]; !exists {
		return 0, nil
	}
	return h.Brstack[key].Len(), nil
}

func (h *Database) Lindex(key string, index int) ([]byte, error) {
	if _, exists := h.Brstack[key]; !exists {
		h.Brstack[key] = NewStack(key)
	}
	return h.Brstack[key].GetIndex(index), nil
}

func (h *Database) Lpush(r *Request, key string, value []byte, values ...[]byte) (int, error) {
	values = append([][]byte{value}, values...)
	k := fmt.Sprintf("%s%d",r.Conn,time.Now().UnixNano())
	Conns.Add(k,make(chan interface{}))
	defer  Conns.Del(k)
	_Storage.Propose("rpush", append([][]byte{[]byte(key)}, values...),k)
	ret, ok := <-Conns.Get(k)
	if !ok {
		return 0, errors.New("rpush op something errors")
	}
	close(Conns.Get(k))
	return ret.(int), nil
}

func (h *Database) Lpop(r *Request, key string) ([]byte, error) {
	if h.Brstack == nil {
		return nil, nil
	}
	if _, found := h.Brstack[key]; !found {
		return nil, nil
	}
	k := fmt.Sprintf("%s%d",r.Conn,time.Now().UnixNano())
	Conns.Add(k,make(chan interface{}))
	defer  Conns.Del(k)
	_Storage.Propose("lpop", append([][]byte{[]byte(key)}), k)
	ret, ok := <-Conns.Get(k)
	if !ok {
		return []byte{}, errors.New("lpop op something errors")
	}
	close(Conns.Get(k))
	return ret.([]byte), nil
}

func (h *Database) Rpop(r *Request, key string) ([]byte, error) {
	if h.Brstack == nil {
		return nil, nil
	}
	if _, found := h.Brstack[key]; !found {
		return nil, nil
	}
	k := fmt.Sprintf("%s%d",r.Conn,time.Now().UnixNano())
	Conns.Add(k,make(chan interface{}))
	defer  Conns.Del(k)
	_Storage.Propose("rpop", append([][]byte{[]byte(key)}),k)

	ret, ok := <-Conns.Get(k)
	if !ok {
		return []byte{}, errors.New("rpop op something errors")
	}
	close(Conns.Get(k))
	return ret.([]byte), nil
}

//set operation
func (h *Database) Sadd(r *Request, key string, values ...string) (int, error) {
	var bytes [][]byte
	for _, value := range values {
		bytes = append(bytes, []byte(value))
	}
	k := fmt.Sprintf("%s%d",r.Conn,time.Now().UnixNano())
	Conns.Add(k,make(chan interface{}))
	defer  Conns.Del(k)
	_Storage.Propose("sadd", append([][]byte{[]byte(key)}, bytes...), k)
	num, ok := <-Conns.Get(k)
	if !ok {
		return 0, errors.New("sadd op something errors")
	}
	close(Conns.Get(k))
	return num.(int), nil
}

func (h *Database) Scard(key string) (int, error) {
	if _, exists := h.Hvset[key]; !exists {
		return 0, nil
	}
	return h.Hvset[key].Len(), nil
}

func (h *Database) Smembers(key string) ([][]byte, error) {
	if _, exists := h.Hvset[key]; !exists {
		return nil, nil
	}
	return *h.Hvset[key].Members(), nil
}

//hash set
func (h *Database) Hget(key, subkey string) ([]byte, error) {
	if h.Hvalues == nil {
		return nil, nil
	}
	if v, exists := h.Hvalues[key]; exists {
		if v, exists := v[subkey]; exists {
			return v, nil
		}
	}
	return nil, nil
}

func (h *Database) Hset(r *Request, key, subkey string, value []byte) (int, error) {

	k := fmt.Sprintf("%s%d",r.Conn,time.Now().UnixNano())
	Conns.Add(k,make(chan interface{}))
	defer  Conns.Del(k)
	_Storage.Propose("hset", append([][]byte{[]byte(key)}, []byte(subkey), value),k)
	num, ok := <-Conns.Get(k)
	if !ok {
		return 0, errors.New("del op something errors")
	}
	close(Conns.Get(k))
	return num.(int), nil
}

func (h *Database) Hgetall(key string) (HashValue, error) {
	if h.Hvalues == nil {
		return nil, nil
	}
	return h.Hvalues[key], nil
}

func (h *Database) Get(key string) ([]byte, error) {
	if h.Values == nil {
		return nil, nil
	}
	return h.Values[key], nil
}

func (h *Database) Set(key string, value []byte) error {
	_Storage.Propose("set", append([][]byte{[]byte(key)}, value), "")
	return nil
}

func (h *Database) Del(r *Request, key string, keys ...string) (int, error) {
	keys = append([]string{key}, keys...)
	var bytes [][]byte
	for _, k := range keys {
		bytes = append(bytes, []byte(k))
	}
	k := fmt.Sprintf("%s%d",r.Conn,time.Now().UnixNano())
	Conns.Add(k,make(chan interface{}))
	defer  Conns.Del(k)
	_Storage.Propose("del", bytes, k)
	num, ok := <-Conns.Get(k)
	if !ok {
		return 0, errors.New("del op something errors")
	}
	close(Conns.Get(k))
	return num.(int), nil
}
func (h *Database) Select(key string) error {
	return nil
}
func (h *Database) Ping() (*StatusReply, error) {
	return &StatusReply{code: "PONG"}, nil
}
