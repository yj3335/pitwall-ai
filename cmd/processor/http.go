package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yj3335/pitwall-ai/internal/pipeline"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

// publishFunc delivers a race-control message to the f1.race_control topic.
type publishFunc func(ctx context.Context, m stream.RaceControlMsg) error

// newHandler serves the live state read API and the race-control injection
// endpoint used by the control plane.
func newHandler(p *pipeline.Pipeline, publish publishFunc) http.Handler {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /v1/tower", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"drivers": p.Board.Active(), "stats": p.Stats()})
	})
	mux.HandleFunc("GET /v1/driver/{n}", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.PathValue("n"))
		s := p.Board.Driver(int32(n))
		if s == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown driver"})
			return
		}
		writeJSON(w, http.StatusOK, s)
	})
	mux.HandleFunc("GET /v1/triggers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, p.RecentTriggers(100))
	})
	mux.HandleFunc("GET /v1/race-control", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, p.RecentRaceControl(50))
	})
	// POST /v1/race-control publishes a race-control message to Kafka. The
	// processor's own consumer reads it back, so it then appears in GET
	// /v1/race-control like any replayed FIA message.
	mux.HandleFunc("POST /v1/race-control", func(w http.ResponseWriter, r *http.Request) {
		if publish == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "publishing disabled"})
			return
		}
		var m stream.RaceControlMsg
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err == nil {
			err = json.Unmarshal(body, &m)
		}
		m.Message = strings.TrimSpace(m.Message)
		if err != nil || m.Message == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be JSON with a non-empty message"})
			return
		}
		if m.TsMs == 0 {
			m.TsMs = time.Now().UnixMilli()
		}
		if m.Category == "" {
			m.Category = "Other"
		}
		if m.SessionKey == 0 { // default to the session currently streaming
			if all := p.Board.Active(); len(all) > 0 {
				m.SessionKey = all[0].SessionKey
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := publish(ctx, m); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, m)
	})
	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, p.Stats()) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	return mux
}
