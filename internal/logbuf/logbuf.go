// Package logbuf keeps this instance's most recent log lines in memory so they
// can be read over HTTP. Render's free tier has no public log URL; GET /logs is
// the public window onto the same JSON lines the process writes to stdout.
package logbuf

import (
	"bytes"
	"sync"
)

// Ring is a fixed-size buffer of complete log lines. It is an io.Writer:
// slog's handlers write exactly one record per Write call.
type Ring struct {
	mu    sync.Mutex
	lines [][]byte
	seqs  []uint64
	next  int
	seq   uint64
}

func New(size int) *Ring {
	return &Ring{lines: make([][]byte, size), seqs: make([]uint64, size)}
}

func (r *Ring) Write(p []byte) (int, error) {
	line := bytes.TrimRight(p, "\n")
	cp := make([]byte, len(line)) // slog reuses its buffer after Write returns
	copy(cp, line)
	r.mu.Lock()
	r.seq++
	r.lines[r.next] = cp
	r.seqs[r.next] = r.seq
	r.next = (r.next + 1) % len(r.lines)
	r.mu.Unlock()
	return len(p), nil
}

// Since returns, oldest first, the newest limit lines with a sequence number
// above after that keep accepts, plus the newest sequence number in the buffer.
// Passing that number back as after polls for lines written since.
func (r *Ring) Since(after uint64, limit int, keep func([]byte) bool) ([][]byte, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]byte
	for i := range r.lines {
		idx := (r.next + i) % len(r.lines) // oldest slot first
		if r.lines[idx] == nil || r.seqs[idx] <= after {
			continue
		}
		if keep != nil && !keep(r.lines[idx]) {
			continue
		}
		out = append(out, r.lines[idx]) // stored lines are never modified, so sharing is safe
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, r.seq
}
