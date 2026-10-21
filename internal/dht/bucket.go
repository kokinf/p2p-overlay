package dht

import (
	"sync"
	"time"

	"p2p-overlay/internal/nodeid"
)

type Bucket struct {
	mu       sync.Mutex
	capacity int
	contacts []*Contact
}

func NewBucket(capacity int) *Bucket {
	return &Bucket{capacity: capacity}
}

func (b *Bucket) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.contacts)
}

func (b *Bucket) Capacity() int { return b.capacity }

func (b *Bucket) Contacts() []*Contact {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*Contact, len(b.contacts))
	copy(out, b.contacts)
	return out
}

func (b *Bucket) Get(id nodeid.ID) *Contact {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.contacts {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (b *Bucket) First() *Contact {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.contacts) == 0 {
		return nil
	}
	return b.contacts[0]
}

func (b *Bucket) MoveToEnd(id nodeid.ID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, c := range b.contacts {
		if c.ID == id {
			c.LastSeen = time.Now()
			b.contacts = append(b.contacts[:i], b.contacts[i+1:]...)
			b.contacts = append(b.contacts, c)
			return true
		}
	}
	return false
}

func (b *Bucket) Append(c *Contact) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.contacts) >= b.capacity {
		return false
	}
	b.contacts = append(b.contacts, c)
	return true
}

func (b *Bucket) ReplaceFirst(c *Contact) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.contacts) == 0 {
		b.contacts = append(b.contacts, c)
		return
	}
	b.contacts = append(b.contacts[1:], c)
}

func (b *Bucket) Remove(id nodeid.ID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, c := range b.contacts {
		if c.ID == id {
			b.contacts = append(b.contacts[:i], b.contacts[i+1:]...)
			return
		}
	}
}
