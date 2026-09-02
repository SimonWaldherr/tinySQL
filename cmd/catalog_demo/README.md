# tinySQL Catalog & Scheduler Demo (`catalog_demo`)

Dieses Beispiel registriert Tabellen, Views und Funktionen im tinySQL-Katalog
und führt wiederkehrende sowie einmalige SQL-Jobs durch die echte tinySQL-
Engine aus. Neben dem kompakten Konsolenmodus gibt es ein Browser-Dashboard für
den laufenden Scheduler.

## Gezeigte Funktionen

- Katalogregistrierung: `RegisterTable`, `RegisterView`, `RegisterFunction`
- Kataloginspektion: `GetTables`, `GetColumns`, `ListJobs`
- `INTERVAL`-Job: SQL in einem festen Millisekundenintervall ausführen
- `ONCE`-Job: SQL zu einem absoluten Zeitpunkt ausführen
- Eigener `JobExecutor`, der SQL parst, ausführt und Ergebnisse protokolliert
- Scheduler-Lebenszyklus: `StartJobScheduler` / `StopJobScheduler`

## Start

```bash
go run ./cmd/catalog_demo

# Browser-Dashboard für den laufenden Scheduler
go run ./cmd/catalog_demo -web -addr 127.0.0.1:8089
# http://localhost:8089
```

Ohne `-web` läuft die Konsolen-Demo etwa sechs Sekunden, druckt Job-Ausgaben,
zeigt den Status und beendet sich anschließend. Dort läuft
`refresh_event_stats` alle zwei Sekunden; `integrity_check` wird ungefähr zwei
Sekunden nach dem Start einmalig ausgeführt.

Mit `-web` bleibt der Scheduler aktiv und zeigt registrierte Tabellen, Job-
Zeitpläne, letzte Ausführungen sowie eine sichere manuelle Ausführung der
bereits registrierten Jobs. Die Oberfläche akzeptiert kein frei eingegebenes
SQL, sondern führt ausschließlich die im Katalog hinterlegten Jobs aus. In
diesem Modus läuft `refresh_event_stats` alle 15 Sekunden und
`integrity_check` einmalig eine Minute nach dem Start. Beide Jobs lassen sich
auch manuell anstoßen.

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-web` | `false` | Browser-Dashboard statt der endlichen Konsolen-Demo starten |
| `-addr` | `127.0.0.1:8089` | HTTP-Adresse im Webmodus; `:8089` für Zugriff von anderen Rechnern |

Der Browsermodus verwendet einen frischen In-memory-Datensatz. Tabellen,
Schedulerstatus und die bis zu 30 zuletzt angezeigten Läufe gehen beim Neustart
verloren.

## Browser-API

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `GET` | `/api/state` | Registrierte Tabellen, Jobstatus und jüngste Läufe |
| `POST` | `/api/jobs/{name}/run` | Einen bereits registrierten Job manuell ausführen |

Beispiel:

```bash
curl -X POST http://127.0.0.1:8089/api/jobs/refresh_event_stats/run
```

Die Anwendung hat keine Anmeldung. Für lokale Nutzung an `127.0.0.1` binden;
vor einem Netzwerkbetrieb Authentifizierung und TLS ergänzen.

## Erwartete Konsolenausgabe (gekürzt)

```
=== tinySQL Catalog & Scheduler Demo ===

✓ Seeded events table with 20 rows

2. Tables registered in catalog:
   - main.events          (type: table, created: 12:00:01)
   - main.event_stats     (type: table, created: 12:00:01)

3. Columns for 'events':
   - id              INT    (position 0, nullable: true)
   ...

6. Creating scheduled jobs...
   - INTERVAL job "refresh_event_stats" every 2s: SELECT kind, COUNT(*) ...
   - ONCE job "integrity_check" at 12:00:02: SELECT COUNT(*) ...

8. Monitoring jobs for 6 seconds (watch log output)...
   job result (2 rows): kind=click, total=13

9. Job status:
   refresh_event_stats       enabled | last: 12:00:06 | next: 12:00:08
   integrity_check           enabled | last: 12:00:02 | next: n/a

=== Demo Complete ===
```

## Verwendete Kern-APIs

```go
catalog := tdb.Catalog()

catalog.RegisterTable("main", "events", []tinysql.Column{...})
catalog.RegisterView("main", "recent_events", "SELECT ...")
catalog.RegisterFunction(&tinysql.CatalogFunction{...})
catalog.RegisterJob(&tinysql.CatalogJob{
    ScheduleType: "INTERVAL",
    IntervalMs:   2000,
})

tdb.StartJobScheduler(executor)
tdb.StopJobScheduler()
```

Die vollständigen APIs stehen in [catalog.go](../../internal/storage/catalog.go)
und [scheduler.go](../../internal/storage/scheduler.go).
