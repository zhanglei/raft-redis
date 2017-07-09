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
		Hvset    :  make(HashSet),
	}
	//db.children[0] = db
	return db
}

func (d *Database)methodSet(b [][]byte)  {
	if d == nil {
		d = NewDatabase()
	}

	d.Values[string(b[0])] = b[1]
}


func (d *Database)methodDel(b [][]byte)  {
	if d == nil {
		d = NewDatabase()
	}

	count := 0
	for _, k := range b {
		key := string(k)
		fmt.Println(key)
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



}
