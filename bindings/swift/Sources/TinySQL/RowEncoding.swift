import Foundation

// Encoding Encodable models into column/value pairs for INSERT.
//
// Property names become column names. Scalars map as documented in
// SQLValue+Foundation.swift; every other Codable value (arrays, nested structs,
// dictionaries) is stored as JSON text, the form RowDecoding reads back.

/// The columns of one encoded record, in declaration order.
struct EncodedRow {
    var columns: [String] = []
    var values: [SQLValue] = []
    private var indices: [String: Int] = [:]

    func value(for column: String) -> SQLValue? {
        indices[column.lowercased()].map { values[$0] }
    }

    mutating func set(_ name: String, _ value: SQLValue) throws {
        let key = name.lowercased()
        guard indices[key] == nil else {
            throw EncodingError.invalidValue(
                name, .init(codingPath: [], debugDescription: "Column \"\(name)\" is encoded twice."))
        }
        indices[key] = columns.count
        columns.append(name)
        values.append(value)
    }
}

/// How property names become column names when writing records.
public enum ColumnNaming: Sendable, Equatable {
    /// The coding key as declared: `createdAt` writes to column `createdAt`.
    case propertyName
    /// `createdAt` writes to `created_at`, `userID` to `user_id`.
    case snakeCase

    func column(for key: String) -> String {
        switch self {
        case .propertyName: return key
        case .snakeCase: return Self.snakeCased(key)
        }
    }

    // Lower-case words separated by underscores; runs of capitals ("URLString")
    // stay together until the last capital that starts a new word.
    static func snakeCased(_ name: String) -> String {
        let characters = Array(name)
        var result = ""
        for (index, character) in characters.enumerated() {
            if character.isUppercase, index > 0 {
                let previous = characters[index - 1]
                let nextIsLower = index + 1 < characters.count && characters[index + 1].isLowercase
                if previous.isLowercase || previous.isNumber || (previous.isUppercase && nextIsLower) {
                    result.append("_")
                }
            }
            result.append(contentsOf: character.lowercased())
        }
        return result
    }
}

enum RowEncoding {
    static func encode<T: Encodable>(_ record: T, naming: ColumnNaming = .propertyName) throws -> EncodedRow {
        let storage = RowStorage(naming: naming)
        try record.encode(to: RowEncoder(storage: storage))
        if let failure = storage.failure { throw failure }
        return storage.row
    }

    /// Table and column names are spliced into SQL text, so they are restricted to
    /// plain identifiers instead of being escaped.
    static func requireIdentifier(_ name: String, kind: String) throws {
        let bytes = Array(name.utf8)
        func isHead(_ c: UInt8) -> Bool { (c >= 0x41 && c <= 0x5A) || (c >= 0x61 && c <= 0x7A) || c == 0x5F }
        guard let head = bytes.first, isHead(head),
              bytes.allSatisfy({ isHead($0) || ($0 >= 0x30 && $0 <= 0x39) }) else {
            throw TinySQLError.invalidInput("\(kind) \"\(name)\" must contain only letters, digits and underscores")
        }
    }
}

final class RowStorage {
    var row = EncodedRow()
    var failure: Error?
    let naming: ColumnNaming
    init(naming: ColumnNaming = .propertyName) { self.naming = naming }
    func fail(_ error: Error) { if failure == nil { failure = error } }
}

struct RowEncoder: Encoder {
    let storage: RowStorage
    var codingPath: [CodingKey] { [] }
    var userInfo: [CodingUserInfoKey: Any] { [:] }

    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(RowKeyedEncoder<Key>(storage: storage))
    }

    func unkeyedContainer() -> UnkeyedEncodingContainer {
        storage.fail(Self.notARecord)
        return RejectedUnkeyedEncoder()
    }

    func singleValueContainer() -> SingleValueEncodingContainer {
        storage.fail(Self.notARecord)
        return RejectedSingleValueEncoder()
    }

    static let notARecord = EncodingError.invalidValue(
        "record", .init(codingPath: [], debugDescription: "A record must encode as a keyed structure (a struct or class)."))
}

private struct RowKeyedEncoder<Key: CodingKey>: KeyedEncodingContainerProtocol {
    let storage: RowStorage
    var codingPath: [CodingKey] { [] }

    private func store(_ key: Key, _ make: () throws -> SQLValue) {
        do { try storage.row.set(storage.naming.column(for: key.stringValue), try make()) } catch { storage.fail(error) }
    }

    func encodeNil(forKey key: Key) throws { store(key) { .null } }
    func encode<T: Encodable>(_ value: T, forKey key: Key) throws { put(value, key) }

    // One path for every requirement, so the primitive overloads below cannot
    // resolve back to themselves.
    private func put<T: Encodable>(_ value: T, _ key: Key) {
        store(key) { try SQLValueEncoding.encode(value, codingPath: [key]) }
    }
    func encode(_ value: Bool, forKey key: Key) throws { put(value, key) }
    func encode(_ value: String, forKey key: Key) throws { put(value, key) }
    func encode(_ value: Double, forKey key: Key) throws { put(value, key) }
    func encode(_ value: Float, forKey key: Key) throws { put(value, key) }
    func encode(_ value: Int, forKey key: Key) throws { put(value, key) }
    func encode(_ value: Int8, forKey key: Key) throws { put(value, key) }
    func encode(_ value: Int16, forKey key: Key) throws { put(value, key) }
    func encode(_ value: Int32, forKey key: Key) throws { put(value, key) }
    func encode(_ value: Int64, forKey key: Key) throws { put(value, key) }
    func encode(_ value: UInt, forKey key: Key) throws { put(value, key) }
    func encode(_ value: UInt8, forKey key: Key) throws { put(value, key) }
    func encode(_ value: UInt16, forKey key: Key) throws { put(value, key) }
    func encode(_ value: UInt32, forKey key: Key) throws { put(value, key) }
    func encode(_ value: UInt64, forKey key: Key) throws { put(value, key) }

    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type, forKey key: Key
    ) -> KeyedEncodingContainer<NestedKey> {
        storage.fail(RowEncoder.notARecord)
        return KeyedEncodingContainer(RowKeyedEncoder<NestedKey>(storage: RowStorage()))
    }

    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        storage.fail(RowEncoder.notARecord)
        return RejectedUnkeyedEncoder()
    }

    func superEncoder() -> Encoder { RowEncoder(storage: storage) }
    func superEncoder(forKey key: Key) -> Encoder { RowEncoder(storage: storage) }
}

private struct RejectedSingleValueEncoder: SingleValueEncodingContainer {
    var codingPath: [CodingKey] { [] }
    mutating func encodeNil() throws {}
    mutating func encode<T: Encodable>(_ value: T) throws {}
}

private struct RejectedUnkeyedEncoder: UnkeyedEncodingContainer {
    var codingPath: [CodingKey] { [] }
    var count: Int { 0 }
    mutating func encodeNil() throws {}
    mutating func encode<T: Encodable>(_ value: T) throws {}
    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        KeyedEncodingContainer(RowKeyedEncoder<NestedKey>(storage: RowStorage()))
    }
    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer { RejectedUnkeyedEncoder() }
    mutating func superEncoder() -> Encoder { RowEncoder(storage: RowStorage()) }
}

enum SQLValueEncoding {
    static func encode<T: Encodable>(_ value: T, codingPath: [CodingKey]) throws -> SQLValue {
        func invalid(_ message: String) -> EncodingError {
            .invalidValue(value, .init(codingPath: codingPath, debugDescription: message))
        }
        switch value {
        case let value as SQLValue:
            return value
        case let value as UInt:
            guard let narrowed = Int64(exactly: value) else { throw invalid("\(value) does not fit a signed 64-bit column.") }
            return .integer(narrowed)
        case let value as UInt64:
            guard let narrowed = Int64(exactly: value) else { throw invalid("\(value) does not fit a signed 64-bit column.") }
            return .integer(narrowed)
        case let value as Double:
            guard value.isFinite else { throw invalid("SQL numbers must be finite.") }
            return .real(value)
        case let value as Float:
            guard value.isFinite else { throw invalid("SQL numbers must be finite.") }
            return .real(Double(value))
        case let value as any SQLValueConvertible:
            return value.sqlValue
        default:
            break
        }

        // Anything else is encoded as JSON. A bare JSON scalar (an enum with a raw
        // value, a single-value wrapper) becomes the matching column type; objects
        // and arrays are stored as JSON text.
        let json: Data
        do {
            json = try JSONEncoder().encode(value)
        } catch {
            throw invalid("Cannot encode \(T.self): \(error)")
        }
        if let fragment = try? JSONDecoder().decode(JSONScalar.self, from: json) {
            switch fragment {
            case .null: return .null
            case .bool(let v): return .boolean(v)
            case .integer(let v): return .integer(v)
            case .real(let v):
                // An integer enum or scalar wrapper can exceed Int64 without
                // being a UInt64 at the call site. Do not round its JSON integer
                // literal through Double after the Int64 probe has failed.
                if json.allSatisfy({ $0 == 0x2D || (0x30...0x39).contains($0) }) {
                    throw invalid("Encoded integer does not fit a signed 64-bit column.")
                }
                return .real(v)
            case .string(let v): return .text(v)
            }
        }
        return .text(String(decoding: json, as: UTF8.self))
    }

    private enum JSONScalar: Decodable {
        case null, bool(Bool), integer(Int64), real(Double), string(String)

        init(from decoder: Decoder) throws {
            let c = try decoder.singleValueContainer()
            if c.decodeNil() { self = .null }
            else if let v = try? c.decode(Bool.self) { self = .bool(v) }
            else if let v = try? c.decode(Int64.self) { self = .integer(v) }
            else if let v = try? c.decode(Double.self) { self = .real(v) }
            else { self = .string(try c.decode(String.self)) }
        }
    }
}
