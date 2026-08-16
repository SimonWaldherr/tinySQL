# tinyORM demo

Beispiel für die additive Migration, Inserts, Auswahlen mit benannten
Parametern und Primärschlüsselzugriffe von [tinyORM](../../tinyorm). Es bietet
eine kurze CLI-Demonstration sowie ein kleines persistentes Ortsverzeichnis im
Browser.

## Zwei Modi

```bash
# Frischer In-memory-Datensatz für die CLI-Demonstration
go run ./cmd/tinyorm_demo
go run ./cmd/tinyorm_demo -include-inactive
go run ./cmd/tinyorm_demo -format json

# Persistentes Ortsverzeichnis
go run ./cmd/tinyorm_demo -web -addr 127.0.0.1:8088
# http://localhost:8088
```

Die CLI erzeugt ihren Datensatz bei jedem Aufruf neu. Der Webmodus öffnet
standardmäßig `places.snapshot`, legt die Beispieldaten beim ersten Start an
und speichert jede Anlage oder Löschung sofort wieder in diesem Snapshot.

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-format` | `text` | CLI-Ausgabe als `text` oder `json` |
| `-include-inactive` | `false` | Auch inaktive Orte in der CLI-Auswahl anzeigen |
| `-web` | `false` | Persistentes Ortsverzeichnis im Browser starten |
| `-addr` | `:8088` | HTTP-Adresse im Webmodus |
| `-snapshot` | `places.snapshot` | Snapshot im Webmodus; leer für nur temporäre Daten |

## Web-Funktionen und API

Der Webmodus demonstriert dieselben ORM-Operationen wie die CLI: `AutoMigrate`,
`Select`, `Insert`, `FindByPK` und `DeleteByPK`. Orte können nach Land und
Aktivstatus gefiltert, angelegt und gelöscht werden.

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `GET` | `/api/places?country=DE&active=active` | Ortsliste; `active` ist `all`, `active` oder `inactive` |
| `POST` | `/api/places` | Ort mit Name, zweistelligem Land, Koordinaten und `active` anlegen |
| `DELETE` | `/api/places/{id}` | Ort löschen |

Die Anwendung hat absichtlich keine Benutzerverwaltung. Für lokale Nutzung an
`127.0.0.1` binden; vor einem Netzwerkbetrieb Authentifizierung und TLS
ergänzen.
