package utils

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type MCond struct {
	L sync.Locker
	n unsafe.Pointer
}

func NewCond(l sync.Locker) *MCond {
	c := &MCond{L: l}
	n := make(chan struct{})
	c.n = unsafe.Pointer(&n)
	return c
}

func (c *MCond) Wait() {
	n := c.NotifyChan()
	c.L.Unlock()
	<-n
	c.L.Lock()
}
func (c *MCond) WaitWithTimeout(t time.Duration) {
	n := c.NotifyChan()
	c.L.Unlock()
	select {
	case <-n:
	case <-time.After(t):
	}
	c.L.Lock()
}
func (c *MCond) NotifyChan() <-chan struct{} {
	ptr := atomic.LoadPointer(&c.n)
	return *((*chan struct{})(ptr))
}
func (c *MCond) Broadcast() {
	n := make(chan struct{})
	ptrOld := atomic.SwapPointer(&c.n, unsafe.Pointer(&n))
	close(*(*chan struct{})(ptrOld))
}
