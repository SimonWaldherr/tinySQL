# Geofence service

Lokaler Geofencing-Dienst mit tinySQL, GeoJSON-Polygonen und einer kleinen
Browser-Oberfläche. Fahrzeuge, Zonen, Positionshistorie und Eintritts- bzw.
Austrittsereignisse werden dauerhaft gespeichert.

## Start

```bash
go run ./cmd/geofence-service -addr 127.0.0.1:8091
# http://localhost:8091
```

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-addr` | `:8091` | HTTP-Adresse; für eine reine lokale Nutzung `127.0.0.1:8091` verwenden |
| `-dsn` | `file:geofence.db?autosave=1` | tinySQL-Datenbank; `mem://` hält Daten nur bis zum Beenden |

Beim ersten Start legt die Anwendung Beispiel-Fahrzeuge und -Zonen an. Neue
Fahrzeuge und GeoJSON-Polygone können im Browser oder über die API angelegt
werden.

## Positionsdaten melden

```bash
curl -X POST http://127.0.0.1:8091/api/positions \
  -H 'Content-Type: application/json' \
  -d '{"vehicle_id":1,"lon":11.58,"lat":48.13}'
```

`recorded_at` ist optional und erwartet einen Unix-Zeitstempel in Sekunden.
Ohne Angabe verwendet der Dienst die aktuelle UTC-Zeit. Längen- und Breitengrad
müssen in den jeweils gültigen Bereichen liegen.

## Ereignisverhalten

Für jede Kombination aus Fahrzeug und Zone hält der Dienst den letzten
Innen-/Außenstatus fest. Der erste Positionspunkt setzt nur diesen Anfangs-
zustand; er erzeugt bewusst kein künstliches Eintrittsereignis. Erst eine
spätere Zustandsänderung schreibt atomar ein `entered`- oder `exited`-Ereignis
in `geofence_events`.

## HTTP-API

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `GET` | `/api/state` | Fahrzeuge, Zonen und die letzten Ereignisse |
| `GET` | `/api/events` | Bis zu 200 jüngste Geofence-Ereignisse |
| `POST` | `/api/vehicles` | Fahrzeug mit `{"name":"…"}` anlegen |
| `POST` | `/api/zones` | Zone mit Name und GeoJSON-Polygon anlegen |
| `POST` | `/api/positions` | Position eines vorhandenen Fahrzeugs speichern |

`POST /api/zones` erwartet zum Beispiel `{"name":"Lager", "geometry":
{"type":"Polygon","coordinates":[…]}}`. Die Geometrie wird vor dem
Speichern als gültiges GeoJSON-Polygon geprüft.

## Betriebshinweis

Die Anwendung hat absichtlich keine Anmeldung. Positionsdaten sind in der
Regel schützenswert: Für lokale Nutzung an `127.0.0.1` binden; für einen
Netzwerkbetrieb Authentifizierung und TLS über einen passenden Reverse Proxy
ergänzen.
