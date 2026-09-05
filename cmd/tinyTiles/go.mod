module github.com/Karte-Bayern/tinyTiles

go 1.26.5

require (
	github.com/SimonWaldherr/tinySQL v0.51.0
	modernc.org/sqlite v1.58.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jonas-p/go-shp v0.1.1 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	golang.org/x/crypto v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	modernc.org/libc v1.75.7 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)

// Keep the standalone command runnable while it lives temporarily below the
// tinySQL worktree. Remove this line after moving it to its own repository and
// replace v0.0.0 with the tinySQL release that exposes the artifact API.
replace github.com/SimonWaldherr/tinySQL => ../..
