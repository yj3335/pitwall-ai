// Package mcp implements a Model Context Protocol server: JSON-RPC 2.0 with
// the initialize / tools/list / tools/call lifecycle, served over stdio
// (newline-delimited JSON), Streamable HTTP (POST /mcp) and the legacy
// HTTP+SSE transport (GET /sse + POST /messages).
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// ProtocolVersion is the MCP revision this server speaks.
const ProtocolVersion = "2025-06-18"

// JSON-RPC error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternal       = -32603
)

// Tool is one callable tool.
type Tool struct {
	Name        string                                                       `json:"name"`
	Description string                                                       `json:"description"`
	InputSchema json.RawMessage                                              `json:"inputSchema"`
	Handler     func(ctx context.Context, args json.RawMessage) (any, error) `json:"-"`
}

// Server routes JSON-RPC requests to tools.
type Server struct {
	Name    string
	Version string
	tools   []Tool
	byName  map[string]*Tool
	Logf    func(format string, args ...any)

	mu       sync.Mutex
	sessions map[string]chan []byte
}

// NewServer creates an empty server.
func NewServer(name, version string) *Server {
	return &Server{Name: name, Version: version, byName: map[string]*Tool{}, sessions: map[string]chan []byte{}, Logf: log.Printf}
}

// AddTool registers a tool.
func (s *Server) AddTool(t Tool) {
	s.tools = append(s.tools, t)
	s.byName[t.Name] = &s.tools[len(s.tools)-1]
	// re-point map entries in case the slice was reallocated
	for i := range s.tools {
		s.byName[s.tools[i].Name] = &s.tools[i]
	}
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// TextContent is an MCP content block.
type TextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// CallResult is the result of tools/call.
type CallResult struct {
	Content           []TextContent `json:"content"`
	StructuredContent any           `json:"structuredContent,omitempty"`
	IsError           bool          `json:"isError"`
}

// Handle processes one JSON-RPC message or batch and returns the encoded
// response, or nil when nothing should be sent (notifications only).
func (s *Server) Handle(ctx context.Context, raw []byte) []byte {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	if raw[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(raw, &batch); err != nil || len(batch) == 0 {
			return mustJSON(response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: CodeInvalidRequest, Message: "invalid batch"}})
		}
		var out []json.RawMessage
		for _, m := range batch {
			if r := s.handleOne(ctx, m); r != nil {
				out = append(out, r)
			}
		}
		if len(out) == 0 {
			return nil
		}
		return mustJSON(out)
	}
	return s.handleOne(ctx, raw)
}

func (s *Server) handleOne(ctx context.Context, raw []byte) []byte {
	var req request
	if err := json.Unmarshal(raw, &req); err != nil {
		return mustJSON(response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: CodeParseError, Message: err.Error()}})
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		return mustJSON(response{JSONRPC: "2.0", ID: nullIfEmpty(req.ID), Error: &rpcError{Code: CodeInvalidRequest, Message: "jsonrpc must be 2.0 with a method"}})
	}
	isNotification := len(req.ID) == 0
	result, rerr := s.dispatch(ctx, req)
	if isNotification {
		return nil
	}
	resp := response{JSONRPC: "2.0", ID: req.ID}
	if rerr != nil {
		resp.Error = rerr
	} else {
		resp.Result = result
	}
	return mustJSON(resp)
}

func (s *Server) dispatch(ctx context.Context, req request) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": s.Name, "version": s.Version},
			"instructions":    "Go Monte Carlo race strategy simulator. Call simulate_pit_strategies to compare pit options over thousands of rollouts.",
		}, nil
	case "notifications/initialized", "notifications/cancelled":
		return nil, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": s.tools}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return nil, &rpcError{Code: CodeInvalidParams, Message: err.Error()}
		}
		t, ok := s.byName[p.Name]
		if !ok {
			return nil, &rpcError{Code: CodeInvalidParams, Message: "unknown tool " + p.Name}
		}
		if len(p.Arguments) == 0 {
			p.Arguments = json.RawMessage("{}")
		}
		start := time.Now()
		out, err := t.Handler(ctx, p.Arguments)
		if err != nil {
			return CallResult{Content: []TextContent{{Type: "text", Text: err.Error()}}, IsError: true}, nil
		}
		if s.Logf != nil {
			s.Logf("tools/call %s %.2fms", p.Name, float64(time.Since(start).Microseconds())/1000)
		}
		return CallResult{Content: []TextContent{{Type: "text", Text: string(mustJSON(out))}}, StructuredContent: out}, nil
	}
	return nil, &rpcError{Code: CodeMethodNotFound, Message: "method not found: " + req.Method}
}

// ServeStdio reads newline-delimited JSON-RPC from r and writes responses to w.
func (s *Server) ServeStdio(ctx context.Context, r io.Reader, w io.Writer) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	bw := bufio.NewWriter(w)
	for sc.Scan() {
		if out := s.Handle(ctx, sc.Bytes()); out != nil {
			bw.Write(out)
			bw.WriteByte('\n')
			if err := bw.Flush(); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// HTTPHandler serves POST /mcp (Streamable HTTP, JSON responses) and the
// legacy GET /sse + POST /messages?session_id= transport.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /mcp", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		out := s.Handle(r.Context(), body)
		if out == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if bytes.Contains(body, []byte(`"initialize"`)) {
			w.Header().Set("Mcp-Session-Id", newID())
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
	})
	mux.HandleFunc("GET /sse", func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		id := newID()
		ch := make(chan []byte, 64)
		s.mu.Lock()
		s.sessions[id] = ch
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			delete(s.sessions, id)
			s.mu.Unlock()
		}()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		fmt.Fprintf(w, "event: endpoint\ndata: /messages?session_id=%s\n\n", id)
		fl.Flush()
		keep := time.NewTicker(15 * time.Second)
		defer keep.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case msg := <-ch:
				fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
				fl.Flush()
			case <-keep.C:
				fmt.Fprint(w, ": keep-alive\n\n")
				fl.Flush()
			}
		}
	})
	mux.HandleFunc("POST /messages", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("session_id")
		s.mu.Lock()
		ch, ok := s.sessions[id]
		s.mu.Unlock()
		if !ok {
			http.Error(w, "unknown session", http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		if out := s.Handle(context.WithoutCancel(r.Context()), body); out != nil {
			select {
			case ch <- out:
			case <-time.After(5 * time.Second):
			}
		}
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		b, _ = json.Marshal(response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: CodeInternal, Message: err.Error()}})
	}
	return b
}

func nullIfEmpty(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

func newID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
