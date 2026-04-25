module github.com/SimonWaldherr/tinySQL/cmd/formigo

go 1.25.9

require (
	github.com/SimonWaldherr/tinySQL v0.0.0
	github.com/microsoft/go-mssqldb v1.9.3
	golang.org/x/crypto v0.50.0
)

require (
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jonas-p/go-shp v0.1.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	golang.org/x/text v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/SimonWaldherr/tinySQL => ../..
