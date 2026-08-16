# geofence-service

Lokaler Geofencing-Dienst mit TinySQL, GeoJSON-Zonen und kleinem Browser-Frontend.

```bash
go run ./cmd/geofence-service
# http://localhost:8091
```

Positionen können auch von Geräten oder Integrationen gemeldet werden:

```bash
curl -X POST http://localhost:8091/api/positions \
  -H 'Content-Type: application/json' \
  -d '{"vehicle_id":1,"lon":11.58,"lat":48.13}'
```

Ein erster Punkt setzt lediglich den Anfangszustand. Danach erzeugt jeder
Eintritt oder Austritt ein dauerhaftes `geofence_events`-Ereignis. Die Daten
liegen standardmäßig in `geofence.db`; mit `-dsn mem://` läuft der Dienst nur
temporär.
