package store

import "fmt"

type (
	HashValue   map[string][]byte
	HashHash    map[string]HashValue
	HashBrStack map[string]*Stack
	HashSet     map[string]*Set
)

type Database struct {
	Values  HashValue
	Hvalues HashHash
	Brstack HashBrStack
	Hvset HashSet
}

func NewDatabase() *Database {
	db := &Database{
		Values:   make(HashValue),
		Brstack:  make(HashBrStack),
		Hvset  :  make(HashSet),
	}
	return db
}

func (d *Database)methodSet(b [][]byte)  {
	if d == nil {
		d = NewDatabase()
	}
	d.Values[string(b[0])] = b[1]
}


func (d *Database)methodDel(b [][]byte) int {
	if d == nil {
		d = NewDatabase()
	}

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

func (d *Database)methodHset(b [][]byte) int {
	ret := 0
	if d == nil {
		d = NewDatabase()
	}

	key := string(b[0])
	subkey := string(b[1])
	value :=  b[2]
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
func (d *Database)methodRpush(b [][]byte) int {

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
func (d *Database)methodLpush(b [][]byte) int {
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
func (d *Database)methodLpop(b [][]byte) []byte {
	key := string(b[0])
	return d.Brstack[key].PopFront()
}

//func (d *Database)methodRpop(key string) []byte {
func (d *Database)methodRpop(b [][]byte) []byte {
	key := string(b[0])
	return d.Brstack[key].PopBack()
}


//func (d *Database)methodSadd(key string, values ...string) int {
func (d *Database)methodSadd(b [][]byte) int {
	count := 0
	key := string(b[0])
	values := b[1:]
	if _, exists := d.Brstack[key]; !exists {
		d.Hvset[key] = NewSet(key)
	}
	fmt.Println("fdfd",key,values)
	for _,value :=range values {
		count =count + d.Hvset[key].Add(string(value))
	}
	return count
}