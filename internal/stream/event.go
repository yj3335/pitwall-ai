// Package stream holds the telemetry event model and the stateful per-driver
// stream engine: rolling fuel-corrected tyre degradation windows and the
// real-time strategy trigger detector.
package stream

import (
	"encoding/json"
	"fmt"
)

// Track status values carried on every event.
const (
	StatusGreen  = "GREEN"
	StatusYellow = "YELLOW"
	StatusSC     = "SC"
	StatusVSC    = "VSC"
	StatusRed    = "RED"
)

// Unknown marks a missing gap/interval measurement in the columnar schema.
const Unknown int64 = -1

// Event is one telemetry sample for one car, the unit carried on the
// f1.telemetry.raw topic (keyed by DriverNumber).
type Event struct {
	TsMs             int64   `json:"ts_ms" parquet:"ts_ms"`
	Seq              int64   `json:"seq" parquet:"seq"`
	SessionKey       int32   `json:"session_key" parquet:"session_key"`
	DriverNumber     int32   `json:"driver_number" parquet:"driver_number"`
	DriverCode       string  `json:"driver_code" parquet:"driver_code,dict"`
	LapNumber        int32   `json:"lap_number" parquet:"lap_number"`
	Sector           int32   `json:"sector" parquet:"sector"`
	Position         int32   `json:"position" parquet:"position"`
	SpeedKph         int32   `json:"speed_kph" parquet:"speed_kph"`
	RPM              int32   `json:"rpm" parquet:"rpm"`
	ThrottlePct      int32   `json:"throttle_pct" parquet:"throttle_pct"`
	Brake            int32   `json:"brake" parquet:"brake"`
	NGear            int32   `json:"n_gear" parquet:"n_gear"`
	DRS              int32   `json:"drs" parquet:"drs"`
	Compound         string  `json:"compound" parquet:"compound,dict"`
	TyreAgeLaps      int32   `json:"tyre_age_laps" parquet:"tyre_age_laps"`
	LapTimeMs        int64   `json:"lap_time_ms" parquet:"lap_time_ms"`
	GapToLeaderMs    int64   `json:"gap_to_leader_ms" parquet:"gap_to_leader_ms"`
	IntervalAheadMs  int64   `json:"interval_ahead_ms" parquet:"interval_ahead_ms"`
	IntervalBehindMs int64   `json:"interval_behind_ms" parquet:"interval_behind_ms"`
	IsPitOutLap      bool    `json:"is_pit_out_lap" parquet:"is_pit_out_lap"`
	TrackTempC       float64 `json:"track_temp_c" parquet:"track_temp_c"`
	Rainfall         int32   `json:"rainfall" parquet:"rainfall"`
	TrackStatus      string  `json:"track_status" parquet:"track_status,dict"`
	IngestMs         int64   `json:"ingest_ms,omitempty" parquet:"ingest_ms"`
}

// wireEvent mirrors Event but with nullable gaps, matching the JSON produced
// by scripts/fetch-openf1.py (OpenF1 reports missing intervals as null).
type wireEvent struct {
	Event
	GapToLeaderMs    *int64   `json:"gap_to_leader_ms"`
	IntervalAheadMs  *int64   `json:"interval_ahead_ms"`
	IntervalBehindMs *int64   `json:"interval_behind_ms"`
	TrackTempC       *float64 `json:"track_temp_c"`
}

func orUnknown(v *int64) int64 {
	if v == nil {
		return Unknown
	}
	return *v
}

// DecodeEvent parses one JSON event, mapping null measurements to Unknown.
func DecodeEvent(b []byte) (Event, error) {
	var w wireEvent
	if err := json.Unmarshal(b, &w); err != nil {
		return Event{}, err
	}
	e := w.Event
	e.GapToLeaderMs = orUnknown(w.GapToLeaderMs)
	e.IntervalAheadMs = orUnknown(w.IntervalAheadMs)
	e.IntervalBehindMs = orUnknown(w.IntervalBehindMs)
	if w.TrackTempC != nil {
		e.TrackTempC = *w.TrackTempC
	}
	if e.DriverNumber <= 0 || e.DriverNumber >= MaxDriverNumber {
		return Event{}, fmt.Errorf("driver_number %d out of range", e.DriverNumber)
	}
	return e, nil
}

// Encode serialises the event for the wire.
func (e Event) Encode() []byte {
	b, _ := json.Marshal(e)
	return b
}

// IsSlick reports whether a compound is a dry-weather tyre.
func IsSlick(compound string) bool {
	switch compound {
	case "SOFT", "MEDIUM", "HARD":
		return true
	}
	return false
}

// Neutralised reports whether the track status slows the whole field.
func Neutralised(status string) bool {
	return status == StatusSC || status == StatusVSC || status == StatusRed
}

// PartitionKey implements the lakehouse hive partition (session=<key>).
func (e Event) PartitionKey() int32 { return e.SessionKey }
