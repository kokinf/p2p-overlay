package dht

import (
	"context"
	"errors"
	"net"
	"sort"
	"time"

	"p2p-overlay/internal/nodeid"
)

const Alpha = 3

type FindNodeFunc func(ctx context.Context, c *Contact, target nodeid.ID) ([]*Contact, error)

type IterationTrace struct {
	Index    int      `json:"index"`
	Queried  []string `json:"queried"`
	Replies  int      `json:"replies"`
	Errors   int      `json:"errors"`
	Timeouts int      `json:"timeouts"`
}

type LookupResult struct {
	Target     nodeid.ID        `json:"target"`
	Closest    []string         `json:"closest"`
	RPCs       int              `json:"rpcs"`
	Iterations int              `json:"iterations"`
	Timeouts   int              `json:"timeouts"`
	Errors     int              `json:"errors"`
	Trace      []IterationTrace `json:"trace"`
	Started    time.Time        `json:"started"`
	Finished   time.Time        `json:"finished"`
	DurationMS int64            `json:"duration_ms"`
}

func IterativeLookup(ctx context.Context, rt *RoutingTable, target nodeid.ID, find FindNodeFunc) *LookupResult {
	started := time.Now()
	res := &LookupResult{Target: target, Started: started}

	queried := make(map[nodeid.ID]bool)
	queried[rt.Self()] = true

	var shortlist []*Contact
	shortlist = append(shortlist, rt.Closest(target, rt.K())...)

	for {
		sort.Slice(shortlist, func(i, j int) bool {
			return nodeid.Less(shortlist[i].ID, shortlist[j].ID, target)
		})

		var batch []*Contact
		for _, c := range shortlist {
			if len(batch) >= Alpha {
				break
			}
			if queried[c.ID] {
				continue
			}
			batch = append(batch, c)
		}
		if len(batch) == 0 {
			break
		}

		trace := IterationTrace{Index: len(res.Trace)}
		type reply struct {
			contact  *Contact
			contacts []*Contact
			err      error
		}
		ch := make(chan reply, len(batch))
		for _, c := range batch {
			queried[c.ID] = true
			trace.Queried = append(trace.Queried, c.ID.String())
			go func(c *Contact) {
				contacts, err := find(ctx, c, target)
				ch <- reply{contact: c, contacts: contacts, err: err}
			}(c)
		}

		for range batch {
			r := <-ch
			res.RPCs++
			if r.err != nil {
				res.Errors++
				if isTimeout(r.err) || errors.Is(r.err, context.DeadlineExceeded) {
					res.Timeouts++
					trace.Timeouts++
				}
				trace.Errors++
				shortlist = removeContact(shortlist, r.contact.ID)
				continue
			}
			trace.Replies++
			rt.Touch(r.contact.ID)
			for _, nc := range r.contacts {
				if nc.ID == rt.Self() {
					continue
				}
				if !containsContact(shortlist, nc.ID) {
					shortlist = append(shortlist, nc)
				}
			}
			sort.Slice(shortlist, func(i, j int) bool {
				return nodeid.Less(shortlist[i].ID, shortlist[j].ID, target)
			})
			if len(shortlist) > 2*rt.K() {
				shortlist = shortlist[:2*rt.K()]
			}
		}
		res.Trace = append(res.Trace, trace)
	}

	sort.Slice(shortlist, func(i, j int) bool {
		return nodeid.Less(shortlist[i].ID, shortlist[j].ID, target)
	})
	if len(shortlist) > rt.K() {
		shortlist = shortlist[:rt.K()]
	}
	res.Iterations = len(res.Trace)
	res.Closest = make([]string, len(shortlist))
	for i, c := range shortlist {
		res.Closest[i] = c.ID.String()
	}
	res.Finished = time.Now()
	res.DurationMS = res.Finished.Sub(res.Started).Milliseconds()
	return res
}

func containsContact(list []*Contact, id nodeid.ID) bool {
	for _, c := range list {
		if c.ID == id {
			return true
		}
	}
	return false
}

func removeContact(list []*Contact, id nodeid.ID) []*Contact {
	out := list[:0]
	for _, c := range list {
		if c.ID != id {
			out = append(out, c)
		}
	}
	return out
}

func isTimeout(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return false
}
