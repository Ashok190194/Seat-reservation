package api

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ctxKey int

const (
	ctxRequestID ctxKey = iota
	ctxReqInfo
)

// reqInfo is filled in by handlers so the access log can carry domain context.
type reqInfo struct {
	mu      sync.Mutex
	userID  string
	outcome string
}

func (ri *reqInfo) set(user, outcome string) {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	if user != "" {
		ri.userID = user
	}
	if outcome != "" {
		ri.outcome = outcome
	}
}

func RequestID(r *http.Request) string {
	id, _ := r.Context().Value(ctxRequestID).(string)
	return id
}

func annotate(r *http.Request, user, outcome string) {
	if ri, ok := r.Context().Value(ctxReqInfo).(*reqInfo); ok {
		ri.set(user, outcome)
	}
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// observe wraps every request with: request id propagation, panic recovery,
// Prometheus HTTP metrics, and one structured access-log line.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-ID")
		if !requestIDPattern.MatchString(rid) {
			rid = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", rid)
		info := &reqInfo{}
		ctx := context.WithValue(r.Context(), ctxRequestID, rid)
		ctx = context.WithValue(ctx, ctxReqInfo, info)
		r = r.WithContext(ctx)

		sw := &statusWriter{ResponseWriter: w}
		s.metrics.HTTPInFlight.Inc()
		defer s.metrics.HTTPInFlight.Dec()

		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic recovered", "request_id", rid, "panic", rec, "stack", string(debug.Stack()))
				if sw.status == 0 {
					writeError(sw, http.StatusInternalServerError, "internal_error", "unexpected server error")
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			dur := time.Since(start)
			s.metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(sw.status)).Inc()
			s.metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(dur.Seconds())

			info.mu.Lock()
			user, outcome := info.userID, info.outcome
			info.mu.Unlock()
			attrs := []any{
				"request_id", rid,
				"method", r.Method,
				"path", r.URL.Path,
				"route", route,
				"status", sw.status,
				"duration_ms", float64(dur.Microseconds()) / 1000,
				"bytes", sw.bytes,
				"remote", r.RemoteAddr,
			}
			if user != "" {
				attrs = append(attrs, "user_id", user)
			}
			if outcome != "" {
				attrs = append(attrs, "outcome", outcome)
			}
			level := slog.LevelInfo
			if sw.status >= 500 {
				level = slog.LevelError
			}
			if route == "GET /metrics" || route == "GET /healthz" || route == "GET /readyz" {
				level = slog.LevelDebug
			}
			s.log.Log(r.Context(), level, "request", attrs...)
		}()

		next.ServeHTTP(sw, r)
	})
}
