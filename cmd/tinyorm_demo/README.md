# tinyORM Demo

This small executable demonstrates tinyORM's additive migration, inserts,
named-parameter selection, and primary-key lookup without any external service.

```bash
go run ./cmd/tinyorm_demo
go run ./cmd/tinyorm_demo -include-inactive
go run ./cmd/tinyorm_demo -format json
```

`-format json` is intended for scripts; the default text format is meant for
interactive exploration. The dataset is in-memory and recreated on each run.
