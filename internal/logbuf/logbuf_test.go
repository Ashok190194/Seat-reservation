package logbuf

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"
)

func TestRingKeepsNewestLinesInOrder(t *testing.T) {
	r := New(3)
	for i := 1; i <= 5; i++ {
		fmt.Fprintf(r, "{\"n\":%d}\n", i)
	}
	lines, next := r.Since(0, 10, nil)
	if next != 5 || len(lines) != 3 || string(lines[0]) != `{"n":3}` || string(lines[2]) != `{"n":5}` {
		t.Fatalf("next=%d lines=%q", next, lines)
	}
	// Polling with the returned sequence number yields only newer lines.
	fmt.Fprint(r, "{\"n\":6}\n")
	if lines, next = r.Since(next, 10, nil); next != 6 || len(lines) != 1 || string(lines[0]) != `{"n":6}` {
		t.Fatalf("poll: next=%d lines=%q", next, lines)
	}
	if lines, _ = r.Since(0, 2, nil); len(lines) != 2 || string(lines[1]) != `{"n":6}` {
		t.Fatalf("limit keeps the newest: %q", lines)
	}
}

func TestRingFiltersAndCapturesSlogRecords(t *testing.T) {
	r := New(10)
	log := slog.New(slog.NewJSONHandler(r, nil))
	log.Info("request", "request_id", "abc", "status", 409)
	log.Info("request", "request_id", "xyz", "status", 201)
	lines, _ := r.Since(0, 10, func(l []byte) bool { return bytes.Contains(l, []byte(`"request_id":"xyz"`)) })
	if len(lines) != 1 || !bytes.Contains(lines[0], []byte(`"status":201`)) {
		t.Fatalf("filtered lines = %q", lines)
	}
}
