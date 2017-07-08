package store

import "fmt"

type (
	HashValue   map[string][]byte
	HashHash    map[string]HashValue
	HashBrStack map[string]*Stack
	HashSet     map[string]*Set
)

type Database struct {
	values  HashValue
	hvalues HashHash
	brstack HashBrStack
	set HashSet
}

func NewDatabase() *Database {
	db := &Database{
		values:   make(HashValue),
		brstack:  make(HashBrStack),
		set    :  make(HashSet),
	}
	//db.children[0] = db
	return db
}

func (d *Database)methodSet(b [][]byte)  {
	if d == nil {
		d = NewDatabase()
	}

	d.values[string(b[0])] = b[1]
}


func (d *Database)methodDel(b [][]byte)  {
	if d == nil {
		d = NewDatabase()
	}


	count := 0
	for _, k := range b {
		key := string(k)
		fmt.Println(key)
		if _, exists := d.values[key]; exists {
			delete(d.values, key)
			count++
		}
		if _, exists := d.hvalues[key]; exists {
			delete(d.hvalues, key)
			count++
		}

		if _, exists := d.set[key]; exists {
			delete(d.set, key)
			count++
		}
	}



}
