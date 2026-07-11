//go:build baremetal

// tinygo-rp2350 shows a tinySQL control loop on RP2350-class TinyGo boards.
//
// Build examples:
//
//	tinygo build -target=pico2 -o tinygo-rp2350.uf2 ./examples/tinygo-rp2350
//	tinygo build -target=xiao-rp2350 -o tinygo-rp2350.uf2 ./examples/tinygo-rp2350
package main

import (
	"context"
	"fmt"
	"machine"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func main() {
	machine.Serial.Configure(machine.UARTConfig{})
	machine.LED.Configure(machine.PinConfig{Mode: machine.PinOutput})

	logf("tinySQL rp2350 demo booting\r\n")

	db := tinysql.NewDB()
	mustExec(db, "CREATE TABLE telemetry (sample_id INT, temperature_c INT, humidity_pct INT)")

	for sampleID := 0; ; sampleID++ {
		temperature := 21 + (sampleID % 5)
		humidity := 44 + ((sampleID * 3) % 11)

		mustExec(db, fmt.Sprintf(
			"INSERT INTO telemetry (sample_id, temperature_c, humidity_pct) VALUES (%d, %d, %d)",
			sampleID, temperature, humidity,
		))

		stats := mustQuery(db, "SELECT COUNT(*) AS samples, AVG(temperature_c) AS avg_temp, MAX(humidity_pct) AS max_humidity FROM telemetry")
		row := stats.Rows[0]
		logf(
			"sample=%d rows=%v avg_temp=%v max_humidity=%v\r\n",
			sampleID,
			row["samples"],
			row["avg_temp"],
			row["max_humidity"],
		)

		pulseLED()
		time.Sleep(2 * time.Second)
	}
}

func mustExec(db *tinysql.DB, sql string) {
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		panic(err)
	}
	if _, err := tinysql.Execute(context.Background(), db, "default", stmt); err != nil {
		panic(err)
	}
}

func mustQuery(db *tinysql.DB, sql string) *tinysql.ResultSet {
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		panic(err)
	}
	result, err := tinysql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		panic(err)
	}
	if len(result.Rows) == 0 {
		panic("expected at least one result row")
	}
	return result
}

func pulseLED() {
	machine.LED.High()
	time.Sleep(120 * time.Millisecond)
	machine.LED.Low()
}

func logf(format string, args ...any) {
	_, _ = fmt.Fprintf(machine.Serial, format, args...)
}
