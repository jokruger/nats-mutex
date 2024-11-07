package natsmutex

import (
	"sync"
	"time"
)

const defaultBucket = "distributed_locks"
const defaultTtl = 30 * time.Second
const defaultBackoff = 10 * time.Millisecond

type singleton struct {
	sync.Mutex
	id string
}

var singletons = make(map[string]*singleton)
var singletonsMutex sync.Mutex

func getSingleton(id string) *singleton {
	singletonsMutex.Lock()
	defer singletonsMutex.Unlock()

	if s, ok := singletons[id]; ok {
		return s
	}

	s := &singleton{id: id}
	singletons[id] = s
	return s
}
