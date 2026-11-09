import Foundation

// Typed access: Codable models in, Codable models out.

extension Database {
    /// Runs a query and decodes each row into `T`. See `QueryResult.decode`.
    ///
    ///     let notes = try await database.query(
    ///         "SELECT * FROM notes WHERE pinned = ? ORDER BY created_at DESC",
    ///         parameters: [true], as: Note.self)
    public func query<T: Decodable & Sendable>(
        _ sql: String, parameters: [SQLValue] = [], as type: T.Type = T.self
    ) throws -> [T] {
        try query(sql, parameters: parameters).decode(type)
    }

    /// Inserts one record into `table`, one column per property. Returns the
    /// number of inserted rows (1). A property that is nil is left out of the
    /// statement, so the column keeps its default (NULL unless declared otherwise).
    ///
    ///     try await database.insert(Note(id: 1, body: "Hello", createdAt: .now), into: "notes")
    ///
    /// Property names are the column names, as `CodingKeys` define them. Pass
    /// `columnNaming: .snakeCase` to write `createdAt` to a `created_at` column;
    /// reading ignores the difference already.
    @discardableResult
    public func insert<T: Encodable & Sendable>(
        _ record: T, into table: String, columnNaming: ColumnNaming = .propertyName
    ) throws -> Int64 {
        try insertAll([record], into: table, columnNaming: columnNaming)
    }

    /// Inserts many records with one prepared statement. Outside a transaction
    /// the batch is atomic: either every record is inserted or none. The columns
    /// are the union of the records' non-nil properties; a record that lacks one
    /// of them inserts NULL there.
    @discardableResult
    public func insertAll<T: Encodable & Sendable>(
        _ records: [T], into table: String, columnNaming: ColumnNaming = .propertyName
    ) throws -> Int64 {
        try RowEncoding.requireIdentifier(table, kind: "Table name")
        let encoded = try records.map { try RowEncoding.encode($0, naming: columnNaming) }
        var columns: [String] = []
        var seen = Set<String>()
        for row in encoded {
            for column in row.columns where seen.insert(column.lowercased()).inserted { columns.append(column) }
        }
        guard !records.isEmpty else { return 0 }
        guard !columns.isEmpty else {
            throw TinySQLError.invalidInput("The record has no properties to insert")
        }
        for column in columns { try RowEncoding.requireIdentifier(column, kind: "Column name") }
        let placeholders = Array(repeating: "?", count: columns.count).joined(separator: ", ")
        let sql = "INSERT INTO \(table) (\(columns.joined(separator: ", "))) VALUES (\(placeholders))"
        let parameterSets: [[SQLValue]] = encoded.map { row in
            columns.map { row.value(for: $0) ?? .null }
        }
        if parameterSets.count == 1 {
            return try execute(sql, parameters: parameterSets[0])
        }
        if isInTransaction {
            return try executeBatch(sql, parameterSets: parameterSets)
        }
        return try transaction { database in
            try database.executeBatch(sql, parameterSets: parameterSets)
        }
    }
}
