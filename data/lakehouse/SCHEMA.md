# Lakehouse layout

The Go stream processor (`cmd/processor`) writes immutable Apache Parquet files
(ZSTD, one row group per file) with a write-to-temp + `rename(2)` commit, so a
reader never observes a partially written file:

```
data/lakehouse/
  telemetry/session=<session_key>/batch_<seq>.parquet   one row per telemetry event
  laps/session=<session_key>/batch_<seq>.parquet        one row per completed lap (+ window stats)
  triggers/session=<session_key>/batch_<seq>.parquet    one row per strategy trigger
```

DuckDB reads them with hive partitioning:
`read_parquet('data/lakehouse/laps/**/*.parquet', hive_partitioning = true)`.

## telemetry

| column | type | notes |
|---|---|---|
| ts_ms | INT64 | event wall-clock time, epoch ms |
| seq | INT64 | global order inside the replayed session |
| session_key | INT32 | OpenF1 session key (also the hive partition) |
| driver_number | INT32 | Kafka partition key |
| driver_code | STRING | three-letter code |
| lap_number, sector, position | INT32 | |
| speed_kph, rpm, throttle_pct, n_gear | INT32 | |
| brake, drs, rainfall | INT32 | 0/1 |
| compound | STRING | SOFT, MEDIUM, HARD, INTERMEDIATE, WET |
| tyre_age_laps | INT32 | |
| lap_time_ms | INT64 | last completed lap |
| gap_to_leader_ms, interval_ahead_ms, interval_behind_ms | INT64 | -1 when unknown |
| track_temp_c | DOUBLE | |
| track_status | STRING | GREEN, YELLOW, SC, VSC, RED |
| ingest_ms | INT64 | processor receive time (for stream latency) |

## laps

| column | type | notes |
|---|---|---|
| session_key, driver_number, lap_number | INT32 | |
| driver_code, compound, track_status | STRING | |
| tyre_age_laps, position | INT32 | |
| lap_time_ms | INT64 | |
| fuel_corrected_ms | INT64 | lap_time_ms + 35 ms x laps since stint start |
| deg_slope_ms_per_lap | DOUBLE | fuel-corrected OLS slope over the last 5 clean laps |
| window_laps | INT32 | clean laps in the window |
| gap_to_leader_ms, interval_ahead_ms, interval_behind_ms | INT64 | |
| is_clean | BOOLEAN | false for in/out laps, SC/VSC/RED laps and >107% outliers |

## triggers

| column | type | notes |
|---|---|---|
| ts_ms, detected_ms | INT64 | event time and detection time |
| session_key, driver_number, lap_number, rival_driver_number | INT32 | |
| kind | STRING | UNDERCUT_WINDOW, TIRE_CLIFF, CHEAP_PIT_WINDOW, WET_CROSSOVER |
| detail | STRING | human-readable reason |
| value | DOUBLE | metric that fired the trigger |
