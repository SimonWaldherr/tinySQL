# worklog

Nutzbare lokale Arbeitszeiterfassung mit kleinem Web-Frontend, Abteilungen,
Mitarbeitenden, Pausen, CSV-Export und kritischen automatischen Ausstempelungen.

```bash
go run ./cmd/worklog
# http://localhost:8094
```

Die Standarddatenbank ist `worklog.db`. `Work`, `Break` und `Clocked out`
sind unveränderliche Statusereignisse. Bleibt ein aktiver Status länger als
zehn Stunden ohne neue Buchung bestehen, schreibt ein Hintergrundjob exakt zum
Timeout-Zeitpunkt einen `system`-Ausstempelvorgang mit `critical=true`.
Nachtschichten werden dabei nicht am Tagesende beendet; die Auswertung teilt
Arbeitszeit nur für Tagesberichte an Mitternacht auf.

`-timeout`, `-sweep` und `-timezone` passen diese Regeln an. Das Beispiel ist
eine lokale Anwendung mit Profilwahl, keine fertige SSO-/Lohnbuchhaltungs-
Integration; für Teambetrieb gehört sie hinter eine passende Authentifizierung.
