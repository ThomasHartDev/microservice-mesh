package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unicode"
)

const MaxDeliver = 3

var (
	ErrClosed  = errors.New("broker closed")
	ErrSubject = errors.New("invalid subject")
	ErrQueue   = errors.New("invalid queue")
)

type Delivery struct {
	Subject string
	Data    []byte
	ack     func()
	nack    func()
}

func (d Delivery) Ack()  { d.ack() }
func (d Delivery) Nack() { d.nack() }

type Handler func(Delivery) error
type sub struct {
	id      int
	pattern string
	queue   string
	handler Handler
}
type Bus struct {
	mu     sync.Mutex
	closed bool
	subs   []*sub
	rr     map[string]int
	nextID int
}

func New() *Bus { return &Bus{rr: make(map[string]int)} }
func ValidSubject(value string, wildcards bool) bool {
	if value == "" {
		return false
	}
	tokens := strings.Split(value, ".")
	for i, tok := range tokens {
		if tok == "" {
			return false
		}
		if tok == ">" {
			return wildcards && i == len(tokens)-1
		}
		if tok == "*" {
			if !wildcards {
				return false
			}
			continue
		}
		for _, r := range tok {
			if r == '*' || r == '>' || unicode.IsSpace(r) {
				return false
			}
		}
	}
	return true
}
func MatchSubject(subject, pattern string) bool {
	if !ValidSubject(subject, false) || !ValidSubject(pattern, true) {
		return false
	}
	s, p, i := strings.Split(subject, "."), strings.Split(pattern, "."), 0
	for _, tok := range p {
		if tok == ">" {
			return i < len(s)
		}
		if i >= len(s) || (tok != "*" && tok != s[i]) {
			return false
		}
		i++
	}
	return i == len(s)
}
func (b *Bus) Publish(ctx context.Context, subject string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if !ValidSubject(subject, false) {
		b.mu.Unlock()
		return ErrSubject
	}
	payload := append([]byte(nil), data...)
	targets := b.pickLocked(subject, "", "")
	b.mu.Unlock()
	for _, s := range targets {
		if err := b.deliver(ctx, s, subject, payload, 1); err != nil {
			return err
		}
	}
	return nil
}
func (b *Bus) Subscribe(pattern string, h Handler, queue string) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	if !ValidSubject(pattern, true) {
		return nil, ErrSubject
	}
	if queue != "" && strings.TrimSpace(queue) == "" {
		return nil, ErrQueue
	}
	b.nextID++
	s := &sub{id: b.nextID, pattern: pattern, queue: queue, handler: h}
	b.subs = append(b.subs, s)
	id := s.id
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i, cur := range b.subs {
			if cur.id == id {
				b.subs = append(b.subs[:i], b.subs[i+1:]...)
				return
			}
		}
	}, nil
}
func (b *Bus) Close() {
	b.mu.Lock()
	b.closed = true
	b.subs = nil
	b.mu.Unlock()
}
func (b *Bus) pickLocked(subject, wantPattern, wantQueue string) []*sub {
	groups := map[string][]*sub{}
	var fanout []*sub
	for _, s := range b.subs {
		if wantPattern != "" && (s.pattern != wantPattern || s.queue != wantQueue) {
			continue
		}
		if !MatchSubject(subject, s.pattern) {
			continue
		}
		if s.queue == "" {
			fanout = append(fanout, s)
			continue
		}
		key := s.pattern + "\x00" + s.queue
		groups[key] = append(groups[key], s)
	}
	targets := fanout
	for key, members := range groups {
		i := b.rr[key] % len(members)
		b.rr[key] = i + 1
		targets = append(targets, members[i])
	}
	return targets
}
func (b *Bus) deliver(ctx context.Context, s *sub, subject string, data []byte, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	nacked := false
	copied := append([]byte(nil), data...)
	d := Delivery{Subject: subject, Data: copied, ack: func() {}, nack: func() { nacked = true }}
	if err := s.handler(d); err != nil {
		nacked = true
	}
	if nacked && attempt < MaxDeliver {
		next := s
		if s.queue != "" {
			b.mu.Lock()
			picked := b.pickLocked(subject, s.pattern, s.queue)
			b.mu.Unlock()
			if len(picked) > 0 {
				next = picked[0]
			}
		}
		return b.deliver(ctx, next, subject, data, attempt+1)
	}
	return nil
}
