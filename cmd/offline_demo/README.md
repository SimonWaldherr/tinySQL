# Offline POI explorer

Kleines Local-first-Beispiel für einen durchsuchbaren POI-Snapshot. Es läuft
ohne SQLite, externen Dienst oder Netzverbindung und kann denselben Snapshot
wahlweise per CLI oder in einem Browser-Explorer durchsuchen.

## Start

```bash
# Flüchtiger Beispieldatensatz im Speicher
go run ./cmd/offline_demo

# Wiederverwendbaren Snapshot erzeugen oder erneut öffnen
go run ./cmd/offline_demo -snapshot /tmp/tinysql-poi.snapshot -query museum

# Maschinenlesbare Ausgabe
go run ./cmd/offline_demo -snapshot /tmp/tinysql-poi.snapshot -json

# Browser-Explorer für denselben Snapshot
go run ./cmd/offline_demo -web -addr 127.0.0.1:8086 \
  -snapshot /tmp/tinysql-poi.snapshot
# http://localhost:8086
```

## Snapshot-Lebenszyklus

Ohne `-snapshot` erzeugt das Programm den kleinen Beispieldatensatz im
Speicher. Mit einem Pfad lädt es einen vorhandenen Snapshot oder erzeugt ihn
beim ersten Aufruf. `-rebuild` ignoriert einen vorhandenen Snapshot und schreibt
den Beispieldatensatz neu. `-read-only` ist standardmäßig `true` und sperrt die
Datenbank nach dem Laden bzw. Anlegen gegen weitere Änderungen.

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-snapshot` | leer | Snapshot-Datei, die erzeugt oder wiederverwendet wird |
| `-query` | `München` | Suche in Name, Stadt oder Kategorie |
| `-json` | `false` | Stabile JSON-Ausgabe statt einer Texttabelle |
| `-rebuild` | `false` | Vorhandenen Snapshot durch Beispieldaten ersetzen |
| `-read-only` | `true` | Schreibzugriffe nach dem Laden bzw. Erzeugen ablehnen |
| `-web` | `false` | Browser-Explorer statt CLI-Ausgabe starten |
| `-addr` | `127.0.0.1:8086` | HTTP-Adresse im Webmodus; `:8086` für Zugriff von anderen Rechnern |

## Browser-API und Datenschutz

Der Webmodus stellt nur lesende Endpunkte bereit:

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `GET` | `/api/status` | Quelle, Read-only-Status und Anzahl der POIs |
| `GET` | `/api/search?q=<begriff>` | Trefferliste; ohne `q` alle POIs |

Die Oberfläche lädt keine externen Kartenkacheln und überträgt den Snapshot
nicht an einen Kartendienst. Ein eventuell sichtbarer OpenStreetMap-Link wird
erst nach einem bewussten Klick geöffnet; dabei sendet der Browser die
Koordinaten an OpenStreetMap. Für lokale Nutzung den Webserver an
`127.0.0.1:8086` binden.

Der kleine Datensatz zeigt nur den Snapshot- und Read-only-Lebenszyklus. Für
größere Kartenbestände sind die dedizierten POI-Index- oder MBTiles-Pfade
vorgesehen.
