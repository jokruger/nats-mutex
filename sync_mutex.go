package natsmutex

type SyncMutex struct {
	m *Mutex
}

func NewSyncMutex(opts ...Option) (*SyncMutex, error) {
	m, err := NewMutex(opts...)
	if err != nil {
		return nil, err
	}
	return &SyncMutex{m: m}, nil
}

func (sm SyncMutex) Lock() {
	if err := sm.m.Lock(); err != nil {
		panic(err)
	}
}

func (sm SyncMutex) TryLock() bool {
	res, err := sm.m.TryLock()
	if err != nil {
		panic(err)
	}
	return res
}

func (sm SyncMutex) Unlock() {
	if err := sm.m.Unlock(); err != nil {
		panic(err)
	}
}
