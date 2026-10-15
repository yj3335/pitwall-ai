package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yj3335/pitwall-ai/internal/stream"
)

func newTestServer(board *stream.Board) *Server {
	s := NewServer("test", "0")
	s.Logf = nil
	RegisterSimTools(s, board)
	return s
}

type rpcResp struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

func call(t *testing.T, s *Server, msg string) rpcResp {
	t.Helper()
	out := s.Handle(context.Background(), []byte(msg))
	var r rpcResp
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatalf("bad response %s: %v", out, err)
	}
	return r
}

const simArgs = `{"total_laps":57,"current_lap":40,"car":{"driver_code":"HAM","position":4,"compound":"HARD","tyre_age_laps":25,"compounds_used":["MEDIUM","HARD"]},"rollouts":10000}`

func TestLifecycleAndToolsList(t *testing.T) {
	s := newTestServer(nil)
	r := call(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`)
	if !strings.Contains(string(r.Result), ProtocolVersion) {
		t.Fatalf("initialize: %s", r.Result)
	}
	if out := s.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)); out != nil {
		t.Fatalf("notification must not get a response: %s", out)
	}
	r = call(t, s, `{"jsonrpc":"2.0","id":"abc","method":"tools/list"}`)
	var list struct {
		Tools []struct {
			Name        string          `json:"name"`
			InputSchema json.RawMessage `json:"inputSchema"`
		} `json:"tools"`
	}
	json.Unmarshal(r.Result, &list)
	names := map[string]bool{}
	for _, tl := range list.Tools {
		names[tl.Name] = true
		var schema map[string]any
		if err := json.Unmarshal(tl.InputSchema, &schema); err != nil || schema["type"] != "object" {
			t.Fatalf("tool %s has invalid schema: %v", tl.Name, err)
		}
	}
	for _, want := range []string{"simulate_pit_strategies", "get_live_driver_window", "compute_undercut_delta", "validate_physics_and_traffic"} {
		if !names[want] {
			t.Fatalf("missing tool %s", want)
		}
	}
	if string(r.ID) != `"abc"` {
		t.Fatalf("id not echoed: %s", r.ID)
	}
}

func TestErrorsAndBatch(t *testing.T) {
	s := newTestServer(nil)
	if r := call(t, s, `{not json`); r.Error == nil || r.Error.Code != CodeParseError {
		t.Fatalf("parse error expected: %+v", r)
	}
	if r := call(t, s, `{"jsonrpc":"1.0","id":1,"method":"ping"}`); r.Error == nil || r.Error.Code != CodeInvalidRequest {
		t.Fatalf("invalid request expected: %+v", r)
	}
	if r := call(t, s, `{"jsonrpc":"2.0","id":1,"method":"bogus"}`); r.Error == nil || r.Error.Code != CodeMethodNotFound {
		t.Fatalf("method not found expected: %+v", r)
	}
	// tool-level failures are results with isError, not protocol errors
	r := call(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"simulate_pit_strategies","arguments":{"total_laps":10,"current_lap":20,"car":{"compound":"HARD"}}}}`)
	if r.Error != nil || !strings.Contains(string(r.Result), `"isError":true`) {
		t.Fatalf("expected isError result: %s", r.Result)
	}
	out := s.Handle(context.Background(), []byte(`[{"jsonrpc":"2.0","id":1,"method":"ping"},{"jsonrpc":"2.0","method":"notifications/initialized"},{"jsonrpc":"2.0","id":2,"method":"tools/list"}]`))
	var batch []rpcResp
	if err := json.Unmarshal(out, &batch); err != nil || len(batch) != 2 {
		t.Fatalf("batch must answer the 2 requests only: %s", out)
	}
}

func TestSimulateOverHTTP(t *testing.T) {
	srv := httptest.NewServer(newTestServer(nil).HTTPHandler())
	defer srv.Close()
	body := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"simulate_pit_strategies","arguments":` + simArgs + `}}`
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ { // concurrent clients
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/mcp", "application/json", strings.NewReader(body))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			var r struct {
				Result CallResult `json:"result"`
			}
			json.NewDecoder(resp.Body).Decode(&r)
			if r.Result.IsError || len(r.Result.Content) == 0 {
				t.Errorf("bad result %+v", r)
				return
			}
			var res struct {
				Rollouts    int    `json:"rollouts_per_strategy"`
				Recommended string `json:"recommended"`
			}
			json.Unmarshal([]byte(r.Result.Content[0].Text), &res)
			if res.Rollouts != 10000 || res.Recommended == "" {
				t.Errorf("unexpected sim result %+v", res)
			}
		}()
	}
	wg.Wait()
	resp, _ := http.Post(srv.URL+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("notification over HTTP should be 202, got %d", resp.StatusCode)
	}
}

func TestLegacySSETransport(t *testing.T) {
	srv := httptest.NewServer(newTestServer(nil).HTTPHandler())
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/sse", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	readEvent := func() (string, string) {
		var ev, data string
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				t.Fatalf("sse read: %v", err)
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case strings.HasPrefix(line, "event: "):
				ev = line[7:]
			case strings.HasPrefix(line, "data: "):
				data = line[6:]
			case line == "" && ev != "":
				return ev, data
			}
		}
	}
	ev, endpoint := readEvent()
	if ev != "endpoint" || !strings.HasPrefix(endpoint, "/messages?session_id=") {
		t.Fatalf("bad endpoint event %q %q", ev, endpoint)
	}
	post, err := http.Post(srv.URL+endpoint, "application/json",
		bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"compute_undercut_delta","arguments":{"gap_ms":1200,"attacker":{"compound":"MEDIUM","tyre_age_laps":15},"defender":{"compound":"MEDIUM","tyre_age_laps":22,"deg_slope_ms_per_lap":110}}}}`)))
	if err != nil || post.StatusCode != http.StatusAccepted {
		t.Fatalf("post: %v %v", err, post)
	}
	ev, data := readEvent()
	if ev != "message" || !strings.Contains(data, `"id":9`) || !strings.Contains(data, "expected_gain_ms") {
		t.Fatalf("bad message event %q %s", ev, data)
	}
}

func TestLiveDriverWindowFromRealStream(t *testing.T) {
	path := filepath.Join("..", "..", "data", "races", "session_9558_events.jsonl.gz")
	if _, err := os.Stat(path); err != nil {
		t.Skip("race data not present")
	}
	events, err := stream.ReadEvents(path)
	if err != nil {
		t.Fatal(err)
	}
	board := stream.NewBoard()
	eng := stream.NewEngine(board)
	for _, ev := range events[:len(events)/2] {
		eng.Process(ev)
	}
	s := newTestServer(board)
	r := call(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_live_driver_window","arguments":{"driver_number":44}}}`)
	var res CallResult
	json.Unmarshal(r.Result, &res)
	if res.IsError {
		t.Fatalf("live window error: %+v", res)
	}
	var w struct {
		Driver   stream.Snapshot `json:"driver"`
		LookupUS int64           `json:"lookup_us"`
	}
	json.Unmarshal([]byte(res.Content[0].Text), &w)
	if w.Driver.DriverCode != "HAM" || w.Driver.LapNumber < 20 || len(w.Driver.CompoundsUsed) == 0 {
		t.Fatalf("unexpected window %+v", w.Driver)
	}
	if w.LookupUS > 1000 {
		t.Fatalf("lookup took %dus, want sub-millisecond", w.LookupUS)
	}
	r = call(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_live_driver_window","arguments":{"driver_number":99}}}`)
	if !strings.Contains(string(r.Result), `"isError":true`) {
		t.Fatalf("unknown driver should be a tool error: %s", r.Result)
	}
}

func TestStdioTransport(t *testing.T) {
	s := newTestServer(nil)
	in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
		`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n")
	var out bytes.Buffer
	if err := s.ServeStdio(context.Background(), in, &out); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 responses (no reply to notification), got %d: %q", len(lines), out.String())
	}
}
