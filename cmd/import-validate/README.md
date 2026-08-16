# import-validate

Lokale Webanwendung für sichere CSV-Imports: Vorschau, Feldregeln,
Fehlerliste und erst danach ein expliziter Schreibvorgang in eine TinySQL-Tabelle.

```bash
go run ./cmd/import-validate
# http://localhost:8093
```

Regeln sind ein JSON-Array. Unterstützt werden `required`, `unique`, `type`
(`text`, `email`, `integer`, `number`, `date`), `min`, `max`, `pattern` und
`target` zum Umbenennen einer Zielspalte. Fehlerhafte Imports bleiben als
prüfbare Historie erhalten und werden niemals in die Zieltabelle geschrieben.
