# Game of Life in tinySQL

```sh
go run ./cmd/gameoflife -width 50 -height 30 -steps 100 -seed 1 -interval 100ms
```

Conway's rules run in SQL: an eight-row offset CTE generates neighbor
contributions, a second CTE groups them, and views calculate the next state.
One atomic stored procedure materializes the next generation before replacing
the current board. Go seeds the board and renders ordered rows in the terminal.
The board wraps around at both edges. The interval is a pause after computation,
not a promise of ten generations per second. Dimensions are limited to 3–200.

This implements the approach in
[SimonWaldherr/GameOfLife's SQL shell demo](https://github.com/SimonWaldherr/GameOfLife/blob/main/cgol.sql.sh).
It is an adaptation, not a drop-in SQLite CLI script:

- `%` and direct joins against views are supported and used in this demo.
- Both `WITH ... INSERT INTO ... SELECT ...` and
  `INSERT INTO ... WITH ... SELECT ...` are supported.
- SQLite CLI directives such as `.mode` and `.separator` belong to the host UI.
- Explicit `x, y, alive` projections preserve positional INSERT column order.

The tests verify a blinker, a stable block and wraparound across an edge:

```sh
go test ./cmd/gameoflife
```
