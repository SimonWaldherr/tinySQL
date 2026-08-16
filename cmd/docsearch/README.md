# docsearch

Durchsuchbare, lokale Dokumentablage mit TinySQL-Volltextsuche und Browser-Oberfläche.

```bash
go run ./cmd/docsearch -docs ./docs
# http://localhost:8092
```

`-docs` bestimmt den ausschließlich lesbaren Ordner. Der Index wird in
`docsearch.db` gespeichert und kann im Browser aktualisiert werden. Indexiert
werden kleine UTF-8-Dateien wie Markdown, Text, HTML, SQL und CSV; `.git`,
`node_modules`, `vendor` und versteckte Ordner werden ausgelassen.
