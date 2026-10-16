package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yj3335/pitwall-ai/internal/pipeline"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

func TestRaceControlInjection(t *testing.T) {
	p := pipeline.New(pipeline.Config{})
	eng := p.NewEngine()
	p.Apply(eng, stream.Event{SessionKey: 9165, DriverNumber: 55, DriverCode: "SAI", LapNumber: 20, Position: 1,
		Compound: "MEDIUM", TrackStatus: stream.StatusGreen, IntervalAheadMs: stream.Unknown})
	var got []stream.RaceControlMsg
	srv := httptest.NewServer(newHandler(p, func(_ context.Context, m stream.RaceControlMsg) error {
		got = append(got, m)
		return nil
	}))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/race-control", "application/json",
		strings.NewReader(`{"message":"VIRTUAL SAFETY CAR DEPLOYED","lap_number":20,"category":"SafetyCar"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var echoed stream.RaceControlMsg
	json.NewDecoder(resp.Body).Decode(&echoed)
	if len(got) != 1 || got[0].SessionKey != 9165 || got[0].TsMs == 0 || echoed.Message != "VIRTUAL SAFETY CAR DEPLOYED" {
		t.Fatalf("published %+v echoed %+v", got, echoed)
	}

	for _, body := range []string{`{}`, `{"message":"   "}`, `not json`} {
		r, _ := http.Post(srv.URL+"/v1/race-control", "application/json", strings.NewReader(body))
		if r.StatusCode != http.StatusBadRequest {
			t.Fatalf("%q: status %d, want 400", body, r.StatusCode)
		}
	}
}

func TestRaceControlPublishFailure(t *testing.T) {
	srv := httptest.NewServer(newHandler(pipeline.New(pipeline.Config{}), func(context.Context, stream.RaceControlMsg) error {
		return errors.New("broker unavailable")
	}))
	defer srv.Close()
	r, _ := http.Post(srv.URL+"/v1/race-control", "application/json", strings.NewReader(`{"message":"RED FLAG"}`))
	if r.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", r.StatusCode)
	}
	nopub := httptest.NewServer(newHandler(pipeline.New(pipeline.Config{}), nil))
	defer nopub.Close()
	r, _ = http.Post(nopub.URL+"/v1/race-control", "application/json", strings.NewReader(`{"message":"RED FLAG"}`))
	if r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", r.StatusCode)
	}
}

func TestReadyzWaitsForPartitionAssignment(t *testing.T) {
	p := pipeline.New(pipeline.Config{})
	srv := httptest.NewServer(newHandler(p, nil))
	defer srv.Close()
	get := func() int {
		r, err := http.Get(srv.URL + "/readyz")
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.StatusCode
	}
	if code := get(); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before assignment: %d", code)
	}
	p.Ready.Store(true) // what the consumer's OnPartitionsAssigned does
	if code := get(); code != http.StatusOK {
		t.Fatalf("readyz after assignment: %d", code)
	}
}
