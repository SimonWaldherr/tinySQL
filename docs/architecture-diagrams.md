# Architecture in diagrams

The same material as [architecture.md](./architecture.md), drawn. Read that one
for the invariants and the prose; read this one to see the shapes.

The diagrams are Mermaid, which GitHub renders inline, as do most Markdown
viewers with Mermaid support.

## The stack

Each layer knows only about the one below it.

```mermaid
flowchart TD
    subgraph app["Your program"]
        A1["tinysql.ParseSQL / Execute"]
        A2["tinysql builder"]
        A3["database/sql + driver"]
    end

    subgraph drv["internal/driver"]
        D1["connector: owns one storage.DB"]
        D2["conn: transactions, placeholder binding"]
        D3["server: reader/writer gating, persist"]
    end

    subgraph eng["internal/engine"]
        E1["lexer + parser to Statement"]
        E2["executeStatement: locks, rollback, WAL, audit"]
        E3["execStmt: dispatch per statement type"]
        E4["fast paths over stored rows"]
        E5["general evaluator over row maps"]
    end

    subgraph sto["internal/storage"]
        S1["DB: tenants, tables"]
        S2["CatalogManager: views, triggers, jobs, RBAC"]
        S3["WALManager / AdvancedWAL"]
        S4["StorageBackend: disk, json, hybrid, index, paged"]
    end

    A1 --> E1
    A2 --> E1
    A3 --> D2
    D2 --> D3
    D1 --> S1
    D2 --> E2
    E1 --> E2
    E2 --> E3
    E3 --> E4
    E3 --> E5
    E4 --> S1
    E5 --> S1
    E2 --> S3
    E3 --> S2
    S1 --> S4
```

The engine touches the filesystem in exactly one place: the SQL-callable
`file()` and `http()` functions in `io_functions.go`. That is a real capability
to be aware of, not an oversight — it gives any statement the process's own
filesystem and network reach.

## Core types

```mermaid
classDiagram
    class DB {
        tenants
        catalog
        wal
        backend
        shadow
        +Get(tenant, name)
        +Put(tenant, table)
        +SnapshotForTx()
        +StatementWAL()
        +copyRuntimeState()
    }
    class Table {
        Name
        Cols
        Rows
        Version
        dirtyFrom
        dirtyRows
        +MarkRowUpdated(idx)
        +MarkDirtyFrom(idx)
        +DirtyRows()
    }
    class Column {
        Name
        Type
        Affinity
        NotNull
        Constraint
        ForeignKey
    }
    class CatalogManager {
        revision
        views
        triggers
        jobs
        rbac
        +Revision()
    }
    class WALManager {
        nextSeq
        checkpointWatermark
        +LogTransaction(changes)
        +Checkpoint(db)
    }
    class StorageBackend {
        <<interface>>
        +LoadTable()
        +SaveTable()
        +Sync()
    }
    class ResultSet {
        Cols
        Rows
    }

    DB "1" --> "many" Table : per tenant
    DB "1" --> "1" CatalogManager
    DB "1" --> "0..1" WALManager
    DB "1" --> "0..1" StorageBackend
    Table "1" --> "many" Column
    Table ..> ResultSet : queried into
```

A `Table` stores rows as `[][]any` — no per-row struct, no page layout. Values
stay native Go values from storage through evaluation into the `ResultSet`, so a
read needs no unmarshalling. That single choice explains most of tinySQL's read
performance and most of its memory profile.

## An autocommit write, end to end

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant Conn as driver.conn
    participant Srv as driver.server
    participant Eng as engine
    participant DB as storage.DB
    participant WAL as WALManager
    participant Disk as disk

    App->>Conn: Exec("UPDATE t SET x = ? WHERE id = ?")
    Conn->>Conn: bind placeholders, parse
    Conn->>Srv: acquire writer slot
    Conn->>Eng: Execute(ctx, live DB, tenant, stmt)
    Eng->>DB: permission check
    Eng->>DB: lock content for write
    Eng->>DB: MetaSnapshot as WAL pre-image
    Eng->>DB: rollback snapshot
    Eng->>DB: apply the change, MarkRowUpdated per row
    Eng->>WAL: LogTransaction(diff of pre-image)
    WAL->>Disk: append records, fsync
    Disk-->>WAL: durable
    alt append failed
        Eng->>DB: restore rollback snapshot
        Eng-->>Conn: error
    else append succeeded
        Eng-->>Conn: result
        Conn->>Srv: persist to backend
        Srv-->>Conn: error propagated on failure
    end
    Conn-->>App: rows affected, or error
```

Two properties are visible here. The log is written **before** the statement is
acknowledged, and a failed append restores the rollback point so memory never
holds a change the log does not. And a persistence failure becomes the
statement's error — acknowledging a write that is not durable is the one outcome
an application cannot detect for itself.

## A transaction

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant Conn as driver.conn
    participant Live as live DB
    participant Shadow as shadow DB
    participant WAL as WALManager

    App->>Conn: BeginTx
    Conn->>Live: SnapshotForTx
    Live-->>Conn: base (versions only) + shadow (deep copy)
    Note over Shadow: marked as a shadow, so StatementWAL<br/>returns nil and statements cannot log
    Conn->>Conn: record catalog revision and contents

    App->>Conn: Exec("INSERT ...")
    Conn->>Shadow: execute
    Note over Shadow: applied in the shadow only,<br/>nothing written to the log

    App->>Conn: Exec("CREATE VIEW ...")
    Conn->>Shadow: execute against the shadow's own catalog copy

    alt COMMIT
        App->>Conn: Commit
        Conn->>Conn: CollectWALChanges(base, shadow)
        Conn->>Conn: catalog changed? revision gate, then compare contents
        Conn->>Live: detectTxConflicts on changed tables
        alt conflict
            Conn-->>App: ErrTransactionConflict (retryable)
        else no conflict
            Conn->>WAL: LogTransaction(all changes) once
            Conn->>Live: ApplyWALChanges
            Conn->>Live: AdoptCatalog if the catalog changed
            Conn-->>App: ok, or the persistence error
        end
    else ROLLBACK or abandoned
        App->>Conn: Rollback, ResetSession, or Close
        Conn->>Shadow: discard
        Note over WAL: nothing to retract, because<br/>the block was never logged
    end
```

The shadow is why a rolled-back transaction leaves nothing behind. When
statements inside a transaction *did* log as they ran, each one landed on disk
looking committed, and recovery resurrected rows a `ROLLBACK` had discarded.

`ModeAdvancedWAL` records row operations as they happen, so it cannot simply stay
silent. It instead joins one ambient WAL transaction per SQL transaction, which
recovery replays only if a matching commit record exists.

## Connection transaction state

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> InTx : BeginTx
    Idle --> InTx : Exec("BEGIN")
    InTx --> InTx : Exec writes to the shadow
    InTx --> Idle : Commit, merged into the live DB
    InTx --> Idle : Rollback, shadow discarded
    InTx --> Idle : ResetSession or Close, shadow discarded
    Idle --> [*] : Close

    note right of InTx
        A transaction started by Exec("BEGIN")
        is invisible to database/sql, so the pool
        can recycle the connection while it is open.
        ResetSession rolls it back; without that the
        connection stayed here forever and silently
        discarded every later write.
    end note
```

## Query execution: fast path or general path

```mermaid
flowchart TD
    Q["SELECT statement"] --> ELIG{"single table,<br/>no CTE reference,<br/>supported shape?"}
    ELIG -- no --> GEN
    ELIG -- yes --> PLAN["compile a plan:<br/>projections, filter, order keys"]
    PLAN --> FILT{"can the WHERE clause<br/>compile to a row filter?"}
    FILT -- "no (nil filter)" --> GEN
    FILT -- yes --> ORD{"can every ORDER BY term<br/>resolve to an output column<br/>or a source column?"}
    ORD -- no --> GEN
    ORD -- yes --> IDX{"does a predicate match<br/>a seekable index,<br/>and is seeking cheaper?"}
    IDX -- yes --> SEEK["index seek,<br/>then evaluate the residual"]
    IDX -- no --> SCAN["scan stored rows,<br/>evaluate the compiled filter"]
    SEEK --> RAW["project from the stored row"]
    SCAN --> RAW
    RAW --> RES["ResultSet"]

    GEN["general path:<br/>build a Row map per row,<br/>recursive evaluator"] --> RES

    style GEN fill:#f5f0e8,stroke:#8a7a5c
    style RAW fill:#e8f0f5,stroke:#5c7a8a
```

A fast path must **decline, never guess**. Every builder returns a plan plus an
`ok`, or a nil filter, and anything it does not fully understand falls through to
the general path. A fast path that returns a subtly wrong answer is the one
failure mode a test cannot catch unless it already knows the right answer.

## Write-ahead log records

One statement or one transaction becomes a `BEGIN`, one record per changed
table, and a `COMMIT`. Recovery applies a transaction's records only when it
reaches that commit record.

```mermaid
flowchart LR
    B["BEGIN<br/>seq, txID"] --> R1
    B --> R2
    B --> R3
    B --> R4
    subgraph per["exactly one of these per changed table"]
        R1["APPEND_ROWS<br/>only the new rows"]
        R2["UPDATE_ROWS<br/>changed rows and their positions"]
        R3["APPLY_TABLE<br/>the whole table"]
        R4["DROP_TABLE"]
    end
    R1 --> C["COMMIT<br/>then fsync"]
    R2 --> C
    R3 --> C
    R4 --> C
```

Which record a change becomes is decided by the dirty tracking on the table:

```mermaid
flowchart TD
    CH["a changed table"] --> D1{"rows only appended?"}
    D1 -- yes --> AR["APPEND_ROWS"]
    D1 -- no --> D2{"exact list of rows<br/>replaced in place,<br/>smaller than the table?"}
    D2 -- yes --> UR["UPDATE_ROWS"]
    D2 -- no --> AT["APPLY_TABLE"]

    style AT fill:#f5e8e8,stroke:#8a5c5c
```

`APPLY_TABLE` is the fallback, and the fallback direction is deliberate. A
mutation that cannot describe its shape gives up the hint, and the cost is one
oversized record. Trusting a hint that is wrong would cost data instead. Before
`UPDATE_ROWS` existed, changing one row of a 10,000-row table serialized and
fsynced all 10,000 of them.

## Recovery

```mermaid
flowchart TD
    OPEN["OpenDB in ModeWAL"] --> CP["load the GOB checkpoint"]
    CP --> WM["read the checkpoint watermark:<br/>the log position it already reflects"]
    WM --> LOOP{"decode the next record"}

    LOOP -- "clean EOF,<br/>whole file consumed" --> DONE["ready"]
    LOOP -- "seq at or below the watermark" --> SKIP["skip, already in the checkpoint"]
    SKIP --> LOOP
    LOOP -- "decode error,<br/>or EOF with bytes left over" --> TORN["truncate at the end of<br/>the last complete record,<br/>report Truncated"]
    TORN --> DONE
    LOOP -- "a record" --> PEND["hold it against its txID"]
    PEND --> ISC{"COMMIT record?"}
    ISC -- no --> LOOP
    ISC -- yes --> APPLY["apply that transaction's records in order"]
    APPLY --> LOOP

    style TORN fill:#f5f0e8,stroke:#8a7a5c
    style SKIP fill:#e8f0f5,stroke:#5c7a8a
```

Both shaded branches exist because of a specific failure. Without the watermark,
a crash between writing a checkpoint and truncating the log replayed
already-checkpointed deltas and **duplicated committed rows**. Without treating a
damaged tail as recoverable, a single bad byte made the database **permanently
unopenable** even though everything before it had recovered fine — and a trailing
run of zero bytes reads as a clean EOF, so anything appended after it would have
been invisible to every later recovery.

## Choosing a storage mode

```mermaid
flowchart TD
    START{"must an acknowledged write<br/>survive a crash?"}
    START -- no --> MEM{"does the data have to<br/>fit in memory?"}
    MEM -- yes --> ModeMemory["ModeMemory<br/>fastest; save explicitly"]
    MEM -- no --> COLD{"what should bound memory?"}
    COLD -- "a byte budget" --> ModeHybrid["ModeHybrid<br/>LRU buffer pool"]
    COLD -- "schema only" --> ModeIndex["ModeIndex<br/>rows on disk"]
    COLD -- "per-table files" --> FMT{"must the files be readable<br/>by a text tool?"}
    FMT -- yes --> ModeJSON["ModeJSON"]
    FMT -- no --> ModeDisk["ModeDisk<br/>GOB per table"]

    START -- yes --> GRAN{"what granularity of<br/>recovery do you need?"}
    GRAN -- "per statement" --> ModeWAL["ModeWAL<br/>log fsynced per commit,<br/>periodic full checkpoint"]
    GRAN -- "per row, point in time" --> ModeAdvancedWAL["ModeAdvancedWAL<br/>row-level log"]

    START -.-> ModePagedIndex["ModePagedIndex<br/>a read-only artifact:<br/>rows and indexes in B+Trees,<br/>immutable"]

    style ModeWAL fill:#e8f5e8,stroke:#5c8a5c
```

`ModeWAL` is the mode to compare against SQLite when an acknowledged write must
survive a crash. Compare equivalent flush tiers: `wal_sync=normal` performs an
ordinary fsync per commit and corresponds to SQLite `synchronous=FULL` without
`fullfsync`; the default `wal_sync=full` uses the strongest available OS flush
(including `F_FULLFSYNC` on macOS) and must be compared with SQLite
`fullfsync=ON`. [BENCHMARKS.md](../BENCHMARKS.md) contains both tiers.

## Where a change belongs

```mermaid
mindmap
  root(("I want to change..."))
    SQL surface
      lexer.go
      parser.go
      parse_*.go
      ast.go
    what a statement does
      exec_dispatch.go
      exec_ddl_*.go
      exec_dml_*.go
      exec_select.go
    functions
      builtin_registry.go
      builtin_string.go
      builtin_math.go
      builtin_datetime.go
      builtin_hash.go
      eval_aggregate.go
      eval_window.go
    performance
      exec_plan.go
      exec_fastpath_*.go
      exec_raw_filter.go
    semantics
      value_semantics.go
      coerce.go
    durability
      wal_manager.go
      wal_advanced.go
      disk_format.go
    transactions
      driver/tx.go
      driver/conn.go
      storage/snapshot.go
```
