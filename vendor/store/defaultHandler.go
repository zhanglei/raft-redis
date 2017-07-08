package store

import (
	"sync"
)


type DefaultHandler struct {
	*Database
	currentDb int
	kv *KvStore
	c *Config
	rwmu sync.RWMutex
}
type Op struct {
	Method string
	Args [][]byte
}

//list operation
func (h *DefaultHandler) Rpush(key string, value []byte, values ...[]byte) (int, error) {

	values = append([][]byte{value}, values...)
	if h.Database == nil {
		h.Database = NewDatabase()
	}
	h.rwmu.Lock()
	defer h.rwmu.Unlock()

	h.kv.Propose("rpush",append([][]byte{[]byte(key)},values...))

	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	for _, value := range values {
		h.brstack[key].PushBack(value)
	}
	return h.brstack[key].Len(), nil
}


func (h *DefaultHandler) Lrange(key string, start, stop int) ([][]byte, error) {
	if h.Database == nil {
		h.Database = NewDatabase()
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
		h.Database = NewDatabase()
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
		h.Database = NewDatabase()
	}
	h.kv.Propose("rpush",append([][]byte{[]byte(key)},values...))
	if _, exists := h.brstack[key]; !exists {
		h.brstack[key] = NewStack(key)
	}
	for _, value := range values {
		h.brstack[key].PushFront(value)
	}
	return h.brstack[key].Len(), nil
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
	h.kv.Propose("lpop",append([][]byte{[]byte(key)}))
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
	h.kv.Propose("rpop",append([][]byte{[]byte(key)}))
	return h.brstack[key].PopBack(),nil
}

//set operation
func (h *DefaultHandler) Sadd (key string, values ...string) (int ,error){
	h.rwmu.Lock()
	defer h.rwmu.Unlock()
	if h.Database == nil {
		h.Database = NewDatabase()
	}

	if _, exists := h.set[key]; !exists {
		h.set[key] = NewSet(key)
	}

	count := 0

	var bytes [][]byte
	for _,value :=range values {
		bytes = append(bytes, []byte(value))
	}
	h.kv.Propose("sadd",append([][]byte{[]byte(key)},bytes...))
	for _,value :=range values {
		count =count + h.set[key].Add(value)
	}
	return count,nil
}


func (h *DefaultHandler) Scard (key string)( int,error) {
	h.rwmu.RLock()
	defer h.rwmu.RUnlock()
	if h.Database == nil {
		h.Database = NewDatabase()
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
		h.Database = NewDatabase()
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


	h.kv.Propose("hset",append([][]byte{[]byte(key)},[]byte(subkey),value))
	if h.Database == nil {
		h.Database = NewDatabase()
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
	h.kv.Propose("set",append([][]byte{[]byte(key)},value))
	/*if h.Database == nil {
		h.Database = NewDatabase()
	}

	h.values[key] = value*/
	return nil
}

func (h *DefaultHandler) Del(key string, keys ...string) (int, error) {

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

	h.kv.Propose("del",bytes)
	//return count.(int), nil
	return 0, nil
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
		h.dbs[index] = NewDatabase()
	}
	h.Database = h.dbs[index]
	return nil
}*/

func NewDefaultHandler( c *Config,kv *KvStore) *DefaultHandler {
	ret := &DefaultHandler{
		kv:kv,
		Database:  kv.Redis,
		currentDb: 0,
		c : c,
	}
	return ret
}
