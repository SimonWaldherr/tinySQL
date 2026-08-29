# import-validate

Lokale Webanwendung für sichere CSV-Imports: Vorschau, Feldregeln,
Fehlerliste und erst danach ein expliziter Schreibvorgang in eine tinySQL-
Tabelle.

## Start

```bash
go run ./cmd/import-validate -addr 127.0.0.1:8093
# http://localhost:8093
```

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-addr` | `:8093` | HTTP-Adresse; für lokale Nutzung `127.0.0.1:8093` verwenden |
| `-dsn` | `file:import-validate.db?autosave=1` | Datenbank für Importhistorie und Zieltabellen |

## Ablauf

1. Zieltabellenname, CSV-Datei und Regeln eingeben.
2. Die Anwendung prüft die vollständige Datei und legt einen nachvollziehbaren
   Importlauf an.
3. Enthält auch nur eine Zeile einen Fehler, erhält der Lauf den Status
   `rejected`; es werden keine Zeilen in eine Zieltabelle geschrieben.
4. Ein vollständig gültiger Lauf hat den Status `validated` und kann erst mit
   der Schaltfläche bzw. `POST /api/imports/{id}/commit` geschrieben werden.

So bleibt die Prüfung von der eigentlichen Datenübernahme getrennt. Auch
fehlerhafte Läufe bleiben mitsamt ihren Fehlern in der Historie sichtbar.

## Regel-Format

Regeln sind ein JSON-Array, beispielsweise:

```json
[
  {"column":"email", "required":true, "type":"email", "unique":true},
  {"column":"amount", "target":"net_amount", "type":"number", "min":0},
  {"column":"order_date", "required":true, "type":"date"}
]
```

Unterstützt werden `required`, `unique`, `type` (`text`, `email`, `integer`,
`number`, `date`), `min`, `max`, `pattern` und `target` zum Umbenennen einer
Zielspalte. Tabellen- und Spaltennamen müssen mit einem Buchstaben oder
Unterstrich beginnen und dürfen danach nur Buchstaben, Ziffern und Unterstriche
enthalten. Die reservierten CSV-Spalten `id`, `source_import_id` und
`source_row` sind nicht erlaubt.

Eine angelegte Zieltabelle enthält zusätzlich genau diese drei Herkunfts-
spalten. Alle importierten Nutzdaten werden als Text gespeichert, damit die
Validierung transparent vom späteren Fachschema getrennt bleibt.

## Grenzen und HTTP-API

Uploads sind auf 5 MiB und 10.000 Datenzeilen begrenzt.

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `POST` | `/api/validate` | Multipart-Upload mit `target`, `rules` und `file` prüfen |
| `GET` | `/api/imports` | Importhistorie laden |
| `GET` | `/api/imports/{id}` | Importlauf und Fehlerdetails laden |
| `POST` | `/api/imports/{id}/commit` | Einen validierten Lauf in die Zieltabelle schreiben |

Die Anwendung hat keine Benutzerverwaltung. Sie sollte deshalb lokal gebunden
oder vor einem externen Zugriff durch Authentifizierung und TLS geschützt
werden.
