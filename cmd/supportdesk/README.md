# tinySQL support desk

Kleine Support-Anwendung mit Wissensbasis, Volltextsuche und Tickets. Sie zeigt
den öffentlichen [`database/sql`-Treiber](../../driver), gebundene Parameter,
Transaktionen, Trigger, Views, CTEs und `FTS_SEARCH` in einem nutzbaren Ablauf.

## Zwei Modi

```bash
# Einmalige Konsolenausgabe mit einem frischen Beispieldatensatz
go run ./cmd/supportdesk
go run ./cmd/supportdesk -query invoice

# Persistente Browser-Oberfläche
go run ./cmd/supportdesk -web -addr 127.0.0.1:8087
# http://localhost:8087
```

Der Konsolenmodus erzeugt bei jedem Aufruf eine In-memory-Datenbank und zeigt
eine Volltextsuche. Der Webmodus verwendet standardmäßig die persistente
`supportdesk.db`: Wissensbasis und angelegte Tickets bleiben daher über einen
Neustart hinweg erhalten.

| Option | Standard | Bedeutung |
| --- | --- | --- |
| `-query` | `password` | Suchbegriff für den Konsolenmodus |
| `-web` | `false` | Browser-Oberfläche starten |
| `-addr` | `:8087` | HTTP-Adresse im Webmodus |
| `-dsn` | `file:supportdesk.db?autosave=1` | Datenbank im Webmodus |

## Web-Funktionen und API

Im Browser lassen sich Artikel durchsuchen, ein passender Artikel auswählen
und daraus ein neues offenes Ticket anlegen. Jeder Ticketvorgang erhält einen
Audit-Eintrag; Artikel und Starttickets werden nur beim ersten Befüllen der
Datenbank angelegt.

| Methode | Pfad | Zweck |
| --- | --- | --- |
| `GET` | `/healthz` | Einfacher Health-Check |
| `GET` | `/api/status` | Anzahl der Artikel und offenen Tickets |
| `GET` | `/api/articles` | Wissensbasis lesen |
| `GET` | `/api/search?q=<begriff>` | Volltextsuche; ohne `q` alle Artikel |
| `GET` | `/api/tickets` | Offene und vorhandene Tickets lesen |
| `POST` | `/api/tickets` | Ticket mit `{"article_id":1,"subject":"…"}` anlegen |

## Betriebshinweis

Die Beispielanwendung hat keine Anmeldung und keine Berechtigungsprüfung.
Insbesondere Tickets dürfen nicht über ein ungeschütztes Netzwerk zugänglich
sein. Für lokale Nutzung an `127.0.0.1` binden; vor einem produktiven Einsatz
Authentifizierung, Autorisierung, TLS und fachliche Ticket-Workflows ergänzen.
