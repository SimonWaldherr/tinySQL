# Stored procedure demo

Run the native demo with:

```bash
go run ./cmd/procedure_demo
```

It registers the same reusable procedures as the WebAssembly playground:

- `demo_table_summary()` and `demo_runtime_status()` for introspection;
- `demo_geo_distance(lat1, lon1, lat2, lon2)` for air-line distance/bearing;
- `demo_find_functions([pattern])` for catalog discovery;
- `demo_log_event(category, message)` for an atomic multi-statement write;
- `demo_release_features()` for the browser feature matrix.

Query `sys.procedures` to inspect descriptions, parameter ranges, read-only and
atomic flags, registrations, call/error counts, last-call timestamps, and
average runtimes.
