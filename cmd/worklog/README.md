# worklog

Nutzbare lokale Arbeitszeiterfassung mit Browser-Oberfläche, Abteilungen,
Mitarbeitenden, Pausen, Tagesberichten, CSV-Export und prüfbaren automatischen
Ausstempelungen.

## Start

```bash
go run ./cmd/worklog -addr 127.0.0.1:8094
# http://localhost:8094
```

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-addr` | `:8094` | HTTP-Adresse; für lokale Nutzung `127.0.0.1:8094` verwenden |
| `-dsn` | `file:worklog.db?autosave=1` | Persistente TinySQL-Datenbank |
| `-timeout` | `10h` | Dauer ohne neue Buchung bis zum automatischen Ausstempeln |
| `-sweep` | `5m` | Intervall, in dem überfällige Status geprüft werden |
| `-timezone` | `Europe/Berlin` | IANA-Zeitzone für Tagesberichte |

Die Anwendung legt zum Einstieg Beispiel-Abteilungen und -Profile an. Über die
Oberfläche lassen sich Profile und Abteilungen ergänzen, ein aktives Profil
einstempeln, Pausen erfassen und Ausstempelungen dokumentieren.

## Zeitmodell und kritische Fälle

`Work`, `Break` und `Clocked out` sind unveränderliche Ereignisse. Ein aktiver
Status läuft weiter, bis ein neues Statusereignis gesetzt wird. Bleibt er länger
als `-timeout` unverändert, schreibt der Hintergrundjob einen `system`-
Ausstempelvorgang exakt auf `letztes Ereignis + timeout`. Dieser Eintrag trägt
`critical=true` und den Grund `timeout_after_inactivity`; er kann im Frontend
mit einer Prüfnotiz als erledigt markiert werden.

`-sweep` beeinflusst nur, wann der überfällige Zustand entdeckt wird, nicht den
gespeicherten Zeitpunkt des Ausstempelns. Der Check läuft auch direkt beim
Start, sodass vergessene Buchungen nach einem Neustart erkannt werden.

Nachtschichten werden nicht an Mitternacht beendet. Für Tagesberichte teilt die
Auswertung ein durchlaufendes Intervall lediglich an der Grenze des in
`-timezone` gewählten Kalendertags auf.

## HTTP-API

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `GET` | `/api/dashboard?user_id=<id>` | Profile, aktueller Status, Historie, Berichte und kritische Fälle |
| `POST` | `/api/stamp` | Statusereignis schreiben |
| `POST` | `/api/departments` | Abteilung anlegen |
| `POST` | `/api/users` | Profil anlegen |
| `POST` | `/api/entries/{id}/resolve` | Kritischen Eintrag mit Prüfnotiz abschließen |
| `GET` | `/api/export?user_id=<id>` | Einträge eines Profils als CSV exportieren |

## Betriebshinweis

Die Profilwahl ist bewusst keine Anmeldung und ersetzt weder SSO noch Lohn-
oder Personalbuchhaltung. Für echte Teams muss die Anwendung hinter einer
geeigneten Authentifizierung, Berechtigungsprüfung und TLS betrieben werden.
