package stream

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ReadEvents loads a (optionally gzipped) JSONL event file produced by
// scripts/fetch-openf1.py.
func ReadEvents(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		r = gz
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var out []Event
	line := 0
	for sc.Scan() {
		line++
		if len(sc.Bytes()) == 0 {
			continue
		}
		ev, err := DecodeEvent(sc.Bytes())
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		out = append(out, ev)
	}
	return out, sc.Err()
}

// RaceControlMsg is one FIA race control / steward message.
type RaceControlMsg struct {
	TsMs         int64  `json:"ts_ms"`
	SessionKey   int32  `json:"session_key"`
	LapNumber    int32  `json:"lap_number"`
	Category     string `json:"category"`
	Flag         string `json:"flag,omitempty"`
	Scope        string `json:"scope,omitempty"`
	DriverNumber int32  `json:"driver_number,omitempty"`
	Message      string `json:"message"`
}

// ReadRaceControl loads data/races/<session>/race_control.json.gz.
func ReadRaceControl(racesDir string, session int32) ([]RaceControlMsg, error) {
	path := filepath.Join(racesDir, fmt.Sprint(session), "race_control.json.gz")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	var raw []struct {
		Date         string `json:"date"`
		SessionKey   int32  `json:"session_key"`
		LapNumber    *int32 `json:"lap_number"`
		Category     string `json:"category"`
		Flag         *string
		Scope        *string
		DriverNumber *int32 `json:"driver_number"`
		Message      string `json:"message"`
	}
	if err := json.NewDecoder(gz).Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]RaceControlMsg, 0, len(raw))
	for _, m := range raw {
		t, err := time.Parse(time.RFC3339Nano, m.Date)
		if err != nil {
			continue
		}
		rc := RaceControlMsg{TsMs: t.UnixMilli(), SessionKey: m.SessionKey, Category: m.Category, Message: m.Message}
		if m.LapNumber != nil {
			rc.LapNumber = *m.LapNumber
		}
		if m.Flag != nil {
			rc.Flag = *m.Flag
		}
		if m.Scope != nil {
			rc.Scope = *m.Scope
		}
		if m.DriverNumber != nil {
			rc.DriverNumber = *m.DriverNumber
		}
		out = append(out, rc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TsMs < out[j].TsMs })
	return out, nil
}
