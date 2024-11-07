# nats-mutex
Nats-mutex is a distributed lock library using NATS.

## Installation

```bash
go get github.com/jokruger/nats-mutex
```

## Basic Usage

Process 1
```go
...
m, _ := pgxmutex.NewSyncMutex(
    pgxmutex.WithUrl("nats://localhost:4222"),
    pgxmutex.WithResourceID("123"),
)
m.Lock()
fmt.Println("Process 1 is in critical section")
time.Sleep(10 * time.Second)
m.Unlock()
...
```

Process 2
```go
...
m, _ := pgxmutex.NewSyncMutex(
    pgxmutex.WithUrl("nats://localhost:4222"),
    pgxmutex.WithResourceID("123"),
)
m.Lock()
fmt.Println("Process 2 is in critical section")
time.Sleep(10 * time.Second)
m.Unlock()
...
```

For more examples and betchmarks refer to https://github.com/jokruger/distributed-lock-benchmark
