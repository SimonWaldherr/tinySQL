module query_files

go 1.25.1

require github.com/SimonWaldherr/tinySQL v0.4.0

require github.com/robfig/cron/v3 v3.0.1 // indirect

replace github.com/SimonWaldherr/tinySQL => ../../../tinySQL
