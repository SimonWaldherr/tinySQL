import Foundation

// Decoding query results into Decodable models.
//
//     struct Note: Codable, Sendable { var id: Int; var body: String; var createdAt: Date }
//     let notes = try await database.query("SELECT * FROM notes", as: Note.self)
//
// Column names are matched to property names ignoring case and underscores, so
// `created_at`, `CREATED_AT` and `createdAt` all select `createdAt`. A missing
// column decodes as nil for optional properties and is an error otherwise.
// Supported column types: all fixed-width integers, Float/Double, Bool, String,
// Data, Date, UUID, URL and RawRepresentable enums of those. Any other Codable
// type (arrays, nested structs, dictionaries) is read from JSON stored in a TEXT
// column, which `Database.insert` writes the same way.

extension QueryResult {
    /// Decodes every row into `T`. Decoding a scalar such as `Int` reads the
    /// first column: `try result.decode(Int.self)` for `SELECT COUNT(*)`.
    public func decode<T: Decodable>(_ type: T.Type = T.self) throws -> [T] {
        let lookup = ColumnLookup(columns)
        return try rows.map { try RowDecoder(row: $0, lookup: lookup, codingPath: []).decode(type) }
    }

    /// Decodes the first row, or returns nil for an empty result.
    public func decodeFirst<T: Decodable>(_ type: T.Type = T.self) throws -> T? {
        guard let first = rows.first else { return nil }
        return try RowDecoder(row: first, lookup: ColumnLookup(columns), codingPath: []).decode(type)
    }
}

struct ColumnLookup: Sendable {
    private let exact: [String: Int]
    private let loose: [String: Int]
    let names: [String]

    init(_ columns: [String]) {
        names = columns
        var exact: [String: Int] = [:]
        var loose: [String: Int] = [:]
        for (index, name) in columns.enumerated() {
            exact[name.lowercased()] = exact[name.lowercased()] ?? index
            let key = Self.looseKey(name)
            loose[key] = loose[key] ?? index
        }
        self.exact = exact
        self.loose = loose
    }

    func index(for key: String) -> Int? {
        exact[key.lowercased()] ?? loose[Self.looseKey(key)]
    }

    private static func looseKey(_ name: String) -> String {
        name.lowercased().replacingOccurrences(of: "_", with: "")
    }
}

/// Thrown by the single-value decoder when a type asks for keyed or unkeyed
/// storage: the value must then be read as JSON text.
private struct NeedsStructuredValue: Error {}

struct RowDecoder: Decoder {
    let row: [SQLValue]
    let lookup: ColumnLookup
    let codingPath: [CodingKey]
    var userInfo: [CodingUserInfoKey: Any] { [:] }

    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        // Foundation's Codable formats differ from their SQL representations
        // (Date uses a 2001 epoch, URL a keyed object, and Data an array).
        switch type {
        case is SQLValue.Type, is Date.Type, is UUID.Type, is URL.Type, is Data.Type:
            return try singleValueContainer().decode(type)
        default:
            break
        }
        return try T(from: self)
    }

    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(RowKeyedContainer<Key>(decoder: self))
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw DecodingError.typeMismatch(
            [Any].self,
            .init(codingPath: codingPath, debugDescription: "A row decodes into a keyed model, not an array."))
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        guard let first = row.first else {
            throw DecodingError.valueNotFound(
                SQLValue.self, .init(codingPath: codingPath, debugDescription: "The result has no columns."))
        }
        return ValueContainer(value: first, codingPath: codingPath)
    }
}

private struct RowKeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    let decoder: RowDecoder
    var codingPath: [CodingKey] { decoder.codingPath }
    var allKeys: [Key] { decoder.lookup.names.compactMap { Key(stringValue: $0) } }

    private func value(_ key: Key) throws -> SQLValue {
        guard let index = decoder.lookup.index(for: key.stringValue), decoder.row.indices.contains(index) else {
            throw DecodingError.keyNotFound(
                key, .init(codingPath: codingPath, debugDescription: "The result has no column \"\(key.stringValue)\"."))
        }
        return decoder.row[index]
    }

    private func path(_ key: Key) -> [CodingKey] { codingPath + [key] }

    func contains(_ key: Key) -> Bool {
        guard let index = decoder.lookup.index(for: key.stringValue) else { return false }
        return decoder.row.indices.contains(index)
    }

    func decodeNil(forKey key: Key) throws -> Bool { try value(key).isNull }

    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        try scalar(type, key)
    }

    // One generic path for every requirement, so the primitive overloads below
    // cannot resolve back to themselves.
    private func scalar<T: Decodable>(_ type: T.Type, _ key: Key) throws -> T {
        try SQLValueDecoding.decode(type, from: try value(key), codingPath: path(key))
    }

    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool { try scalar(Bool.self, key) }
    func decode(_ type: String.Type, forKey key: Key) throws -> String { try scalar(String.self, key) }
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double { try scalar(Double.self, key) }
    func decode(_ type: Float.Type, forKey key: Key) throws -> Float { try scalar(Float.self, key) }
    func decode(_ type: Int.Type, forKey key: Key) throws -> Int { try scalar(Int.self, key) }
    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 { try scalar(Int8.self, key) }
    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 { try scalar(Int16.self, key) }
    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 { try scalar(Int32.self, key) }
    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 { try scalar(Int64.self, key) }
    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt { try scalar(UInt.self, key) }
    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 { try scalar(UInt8.self, key) }
    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 { try scalar(UInt16.self, key) }
    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 { try scalar(UInt32.self, key) }
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { try scalar(UInt64.self, key) }

    func nestedContainer<NestedKey: CodingKey>(
        keyedBy type: NestedKey.Type, forKey key: Key
    ) throws -> KeyedDecodingContainer<NestedKey> {
        throw DecodingError.typeMismatch(
            type, .init(codingPath: path(key), debugDescription: "Columns hold scalars; use a Codable property backed by JSON text."))
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw DecodingError.typeMismatch(
            [Any].self, .init(codingPath: path(key), debugDescription: "Columns hold scalars; use a Codable property backed by JSON text."))
    }

    func superDecoder() throws -> Decoder { decoder }
    func superDecoder(forKey key: Key) throws -> Decoder { decoder }
}

private struct ValueContainer: SingleValueDecodingContainer {
    let value: SQLValue
    let codingPath: [CodingKey]

    func decodeNil() -> Bool { value.isNull }
    func decode<T: Decodable>(_ type: T.Type) throws -> T { try scalar(type) }

    private func scalar<T: Decodable>(_ type: T.Type) throws -> T {
        try SQLValueDecoding.decode(type, from: value, codingPath: codingPath)
    }

    func decode(_ type: Bool.Type) throws -> Bool { try scalar(Bool.self) }
    func decode(_ type: String.Type) throws -> String { try scalar(String.self) }
    func decode(_ type: Double.Type) throws -> Double { try scalar(Double.self) }
    func decode(_ type: Float.Type) throws -> Float { try scalar(Float.self) }
    func decode(_ type: Int.Type) throws -> Int { try scalar(Int.self) }
    func decode(_ type: Int8.Type) throws -> Int8 { try scalar(Int8.self) }
    func decode(_ type: Int16.Type) throws -> Int16 { try scalar(Int16.self) }
    func decode(_ type: Int32.Type) throws -> Int32 { try scalar(Int32.self) }
    func decode(_ type: Int64.Type) throws -> Int64 { try scalar(Int64.self) }
    func decode(_ type: UInt.Type) throws -> UInt { try scalar(UInt.self) }
    func decode(_ type: UInt8.Type) throws -> UInt8 { try scalar(UInt8.self) }
    func decode(_ type: UInt16.Type) throws -> UInt16 { try scalar(UInt16.self) }
    func decode(_ type: UInt32.Type) throws -> UInt32 { try scalar(UInt32.self) }
    func decode(_ type: UInt64.Type) throws -> UInt64 { try scalar(UInt64.self) }
}

/// A decoder over exactly one value, handed to types such as RawRepresentable
/// enums that read themselves from a single value container.
private struct ScalarDecoder: Decoder {
    let value: SQLValue
    let codingPath: [CodingKey]
    var userInfo: [CodingUserInfoKey: Any] { [:] }

    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        throw NeedsStructuredValue()
    }
    func unkeyedContainer() throws -> UnkeyedDecodingContainer { throw NeedsStructuredValue() }
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        ValueContainer(value: value, codingPath: codingPath)
    }
}

enum SQLValueDecoding {
    static func decode<T: Decodable>(_ type: T.Type, from value: SQLValue, codingPath: [CodingKey]) throws -> T {
        // SQLValue already carries its type and can represent NULL itself.
        // Sending it through Codable's primitive probes would coerce integers
        // to Bool and integral REALs to Int64, and treat BLOBs as tagged JSON.
        if type == SQLValue.self { return value as! T }
        func mismatch(_ expected: String) -> DecodingError {
            .typeMismatch(type, .init(codingPath: codingPath, debugDescription: "Expected \(expected), found \(describe(value))."))
        }
        if value.isNull {
            throw DecodingError.valueNotFound(
                type, .init(codingPath: codingPath, debugDescription: "Found NULL; declare the property optional."))
        }
        if let integerType = type as? any (FixedWidthInteger & Decodable).Type {
            return try integer(integerType, value, mismatch) as! T
        }
        switch type {
        case is Double.Type:
            guard let d = value.doubleValue else { throw mismatch("a number") }
            return d as! T
        case is Float.Type:
            guard let d = value.doubleValue else { throw mismatch("a number") }
            return Float(d) as! T
        case is Bool.Type:
            guard let b = value.boolValue else { throw mismatch("a boolean") }
            return b as! T
        case is String.Type:
            guard let s = value.stringValue else { throw mismatch("text") }
            return s as! T
        case is Data.Type:
            guard let d = value.dataValue else { throw mismatch("a BLOB") }
            return d as! T
        case is Date.Type:
            guard let d = value.dateValue else { throw mismatch("an RFC 3339 timestamp") }
            return d as! T
        case is UUID.Type:
            guard let u = value.uuidValue else { throw mismatch("a UUID string") }
            return u as! T
        case is URL.Type:
            guard let u = value.urlValue else { throw mismatch("a URL string") }
            return u as! T
        default:
            break
        }
        do {
            return try T(from: ScalarDecoder(value: value, codingPath: codingPath))
        } catch is NeedsStructuredValue {
            let json: Data
            switch value {
            case .text(let text): json = Data(text.utf8)
            case .blob(let data): json = data
            default: throw mismatch("JSON text")
            }
            do {
                return try JSONDecoder().decode(T.self, from: json)
            } catch {
                throw DecodingError.dataCorrupted(
                    .init(codingPath: codingPath, debugDescription: "Column is not valid JSON for \(T.self).", underlyingError: error))
            }
        }
    }

    private static func integer<I: FixedWidthInteger>(
        _ type: I.Type, _ value: SQLValue, _ mismatch: (String) -> DecodingError
    ) throws -> I {
        switch value {
        case .integer(let v):
            guard let result = I(exactly: v) else { throw mismatch("an integer that fits \(I.self)") }
            return result
        case .real(let v):
            guard let result = I(exactly: v) else { throw mismatch("a whole number that fits \(I.self)") }
            return result
        case .boolean(let v):
            return v ? 1 : 0
        default:
            throw mismatch("an integer")
        }
    }

    private static func describe(_ value: SQLValue) -> String {
        switch value {
        case .null: return "NULL"
        case .integer: return "an integer"
        case .real: return "a real number"
        case .text: return "text"
        case .boolean: return "a boolean"
        case .blob: return "a BLOB"
        }
    }
}
