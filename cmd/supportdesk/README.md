# tinySQL support-desk example

A small, runnable support-search application that keeps its knowledge base,
support tickets, and audit trail in an in-memory tinySQL database.

```bash
go run ./cmd/supportdesk
go run ./cmd/supportdesk -query invoice
```

It demonstrates the public [`database/sql` driver](../../driver), bound query
parameters, a transaction for related writes, an `AFTER INSERT` audit trigger,
an `open_tickets` view, a CTE dashboard query, and `FTS_SEARCH` full-text
search. The data is deliberately seeded so it runs without configuration,
network access, or an external database.
