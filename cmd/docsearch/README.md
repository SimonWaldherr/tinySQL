# docsearch

Durchsuchbare, lokale Dokumentablage mit tinySQL-Volltextsuche und Browser-
Oberfläche. Der Quellordner bleibt ausschließlich lesbar; nur der Suchindex
wird in tinySQL gespeichert.

## Start

```bash
go run ./cmd/docsearch -docs ./docs -addr 127.0.0.1:8092
# http://localhost:8092
```

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-docs` | `.` | Verzeichnis, dessen Dateien indexiert werden |
| `-addr` | `127.0.0.1:8092` | HTTP-Adresse; `:8092` für Zugriff von anderen Rechnern |
| `-dsn` | `file:docsearch.db?autosave=1` | Speicherort des persistenten Suchindexes |

Beim ersten Start wird ein leerer Index aufgebaut. Danach bleibt der Index
erhalten, bis er im Browser oder mit `POST /api/reindex` erneuert wird. Ein
Reindex ersetzt den gesamten Index; dadurch verschwinden auch gelöschte
Dokumente aus den Suchergebnissen.

## Was wird indexiert?

Indexiert werden gültige UTF-8-Dateien bis 1 MiB mit den Endungen `.md`,
`.txt`, `.rst`, `.html`, `.htm`, `.go`, `.sql` und `.csv`. Die Ordner `.git`,
`node_modules`, `vendor` sowie alle versteckten Unterordner werden
übersprungen. Aus jeder Datei werden relativer Pfad, ein Titel und der Inhalt
gespeichert.

## HTTP-API

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `GET` | `/api/status` | Indexgröße und Zeit des jüngsten Dokuments |
| `GET` | `/api/search?q=<begriff>` | Volltextsuche; ohne `q` die neuesten Dokumente |
| `GET` | `/api/documents/{id}` | Vollständigen Inhalt eines indexierten Dokuments lesen |
| `POST` | `/api/reindex` | Quellverzeichnis erneut einlesen |

Die Anwendung dient auch als kleines Beispiel für `FTS_SEARCH`; bei einer
nicht auswertbaren erweiterten Suchanfrage fällt sie auf eine einfache
Textsuche zurück.

## Betriebshinweis

Der Index enthält den Dokumentinhalt und die API kann ihn ausliefern. Deshalb
die Anwendung nur lokal an `127.0.0.1` binden oder vor einem Netzwerkzugriff
mit Authentifizierung und TLS absichern.
