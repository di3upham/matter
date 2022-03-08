package matter

import (
	"container/heap"
	"strconv"
	"sync"
	"time"
)

type Kdb struct {
	*sync.Mutex
	es  []*entry
	em  map[string]*entry
	eh  *entryHeap
	len int
}

func NewKdb() *Kdb {
	return &Kdb{
		Mutex: &sync.Mutex{},
		es:    make([]*entry, 0),
		em:    make(map[string]*entry),
		eh:    &entryHeap{},
	}
}

// required key
// if stateless, append out alias
func (db *Kdb) Upsert(key string, value interface{}) string {
	if key == "" {
		return ""
	}
	db.Lock()
	defer db.Unlock()
	nows := time.Now().Unix()
	e := db.em[key]

	if e != nil {
		if e.t != nows {
			e.t = nows
			heap.Fix(db.eh, e.ih)
		}
		e.v = value
		return strconv.Itoa(e.i)
	}

	e = db.eh.At(0)
	if e != nil && e.k == "" {
		e.k = key
		e.t = nows
		heap.Fix(db.eh, e.ih)
		db.em[key] = e
		e.v = value
		return strconv.Itoa(e.i)
	}

	e = &entry{k: key, t: nows, i: db.len}
	db.len++
	db.es = append(db.es, e)
	db.eh.Push(e)
	db.em[key] = e
	e.v = value
	return strconv.Itoa(e.i)
}

func (db *Kdb) Read(key, alias string) (interface{}, bool) {
	ki, err := strconv.Atoi(alias)
	if err != nil {
		return nil, false
	}
	if ki < 0 || ki >= db.len {
		return nil, false
	}
	e := db.es[ki]
	if e.k == "" || e.k != key {
		return nil, false
	}
	return e.v, true
}

func (db *Kdb) Readi(key string, ki int) (interface{}, bool) {
	if ki < 0 || ki >= db.len {
		return nil, false
	}
	e := db.es[ki]
	if e.k == "" || e.k != key {
		return nil, false
	}
	return e.v, true
}

func (db *Kdb) Delete(key string) {
	if key == "" {
		return
	}
	db.Lock()
	defer db.Unlock()
	e := db.em[key]
	if e != nil {
		e.k = ""
		e.t = 0
		heap.Fix(db.eh, e.ih)
	}
	delete(db.em, key)
}

func (db *Kdb) List(f func(key string, value interface{}) bool) {
	n := db.len
	var e *entry
	for i := 0; i < n; i++ {
		e = db.es[i]
		if e == nil || e.k == "" {
			continue
		}
		if !f(e.k, e.v) {
			break
		}
	}
}

type entry struct {
	i  int
	ih int
	k  string
	v  interface{}
	t  int64
}

type entryHeap []*entry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].t < h[j].t }
func (h entryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].ih, h[j].ih = i, j }

func (h *entryHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*entry))
	ih := len(*h) - 1
	(*h)[ih].ih = ih
}

// don't use
func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *entryHeap) At(i int) *entry {
	arr := *h
	if i >= 0 && i < len(arr) {
		return arr[i]
	}
	return nil
}
