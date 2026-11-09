import Foundation
import XCTest
@testable import TinySQL

private enum Status: String, Codable, Sendable { case open, done }

private struct Note: Codable, Sendable, Equatable {
    var id: Int
    var body: String
    var createdAt: Date
    var pinned: Bool
    var score: Double?
    var status: Status
    var tags: [String]
    var token: UUID
    var link: URL?
    var payload: Data?
}

final class ModelTests: XCTestCase {
    func testSQLValueDecodingPreservesNullAndNumericTypes() throws {
        let values: [SQLValue] = [.null, .integer(7), .integer(Int64.max), .real(3), .boolean(true), .text("text"), .blob(Data([0, 255]))]
        let result = QueryResult(columns: ["value"], rows: values.map { [$0] })
        XCTAssertEqual(try result.decode(SQLValue.self), values)
        XCTAssertEqual(try result.decodeFirst(SQLValue.self), .null)

        struct RawValue: Decodable { let value: SQLValue }
        XCTAssertEqual(try result.decode(RawValue.self).map(\.value), values)
    }

    func testFoundationScalarsDecodeFromTheFirstColumn() throws {
        let date = Date(timeIntervalSince1970: 1_700_000_000.5)
        let identifier = UUID()
        let url = URL(string: "https://example.com/note")!
        let data = Data([0, 255, 7])
        func result(_ value: SQLValue) -> QueryResult {
            QueryResult(columns: ["value"], rows: [[value]])
        }
        XCTAssertEqual(try result(.date(date)).decodeFirst(Date.self), date)
        XCTAssertEqual(try result(.integer(60)).decode(Date.self), [Date(timeIntervalSince1970: 60)])
        XCTAssertEqual(try result(.uuid(identifier)).decode(UUID.self), [identifier])
        XCTAssertEqual(try result(.url(url)).decode(URL.self), [url])
        XCTAssertEqual(try result(.blob(data)).decode(Data.self), [data])
    }

    func testEncodingRejectsCaseInsensitiveColumnCollisions() throws {
        struct Collision: Encodable {
            let id = 1
            let ID = 2
        }
        XCTAssertThrowsError(try RowEncoding.encode(Collision()))
    }

    func testBatchColumnUnionMatchesSQLIdentifierCase() async throws {
        struct Record: Encodable, Sendable {
            let value: String
            let uppercase: Bool
            enum CodingKeys: String, CodingKey { case name, NAME }
            func encode(to encoder: Encoder) throws {
                var container = encoder.container(keyedBy: CodingKeys.self)
                try container.encode(value, forKey: uppercase ? .NAME : .name)
            }
        }
        let database = try Database()
        try await database.execute("CREATE TABLE records (name TEXT)")
        try await database.insertAll([Record(value: "first", uppercase: false), Record(value: "second", uppercase: true)], into: "records")
        let names = try await database.query("SELECT name FROM records", as: String.self)
        XCTAssertEqual(names, ["first", "second"])
        try await database.close()
    }

    private func sample(_ id: Int) -> Note {
        Note(id: id, body: "Grüße 🦊 \(id)", createdAt: Date(timeIntervalSince1970: 1_700_000_000.25 + Double(id)),
             pinned: id % 2 == 0, score: id == 1 ? nil : 2.5, status: id == 1 ? .open : .done,
             tags: ["a", "b \"quoted\""], token: UUID(), link: URL(string: "https://example.com/\(id)"),
             payload: id == 1 ? nil : Data([0, 255, 7]))
    }

    private func makeDatabase() async throws -> Database {
        let database = try Database()
        try await database.execute("""
            CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, created_at TEXT, pinned BOOL, score FLOAT,
                                status TEXT, tags TEXT, token TEXT, link TEXT, payload BLOB)
            """)
        return database
    }

    func testRecordsRoundTripThroughInsertAndTypedQuery() async throws {
        let database = try await makeDatabase()
        let notes = (1...3).map(sample)
        try await database.insert(notes[0], into: "notes", columnNaming: .snakeCase)
        let inserted = try await database.insertAll(Array(notes[1...]), into: "notes", columnNaming: .snakeCase)
        XCTAssertEqual(inserted, 2)

        let loaded = try await database.query("SELECT * FROM notes ORDER BY id", as: Note.self)
        XCTAssertEqual(loaded.count, 3)
        for (original, copy) in zip(notes, loaded) {
            XCTAssertEqual(copy.id, original.id)
            XCTAssertEqual(copy.body, original.body)
            XCTAssertEqual(copy.createdAt.timeIntervalSince1970, original.createdAt.timeIntervalSince1970, accuracy: 1e-6)
            XCTAssertEqual(copy.pinned, original.pinned)
            XCTAssertEqual(copy.score, original.score)
            XCTAssertEqual(copy.status, original.status)
            XCTAssertEqual(copy.tags, original.tags)
            XCTAssertEqual(copy.token, original.token)
            XCTAssertEqual(copy.link, original.link)
            XCTAssertEqual(copy.payload, original.payload)
        }

        let ids = try await database.query("SELECT id FROM notes ORDER BY id").decode(Int.self)
        XCTAssertEqual(ids, [1, 2, 3])
        let count = try await database.query("SELECT COUNT(*) FROM notes").decodeFirst(Int.self)
        XCTAssertEqual(count, 3)
        let none = try await database.query("SELECT id FROM notes WHERE id > 99").decodeFirst(Int.self)
        XCTAssertNil(none)
        try await database.close()
    }

    func testInsertAllIsAtomicAndValidatesNames() async throws {
        let database = try await makeDatabase()
        let first = sample(1)
        var duplicate = sample(2)
        duplicate.id = 1
        do {
            try await database.insertAll([first, duplicate], into: "notes", columnNaming: .snakeCase)
            XCTFail("A duplicate primary key was accepted")
        } catch TinySQLError.database { }
        let remaining = try await database.query("SELECT id FROM notes")
        XCTAssertTrue(remaining.rows.isEmpty, "A failed batch must not leave earlier records behind")
        let inTransaction = await database.isInTransaction
        XCTAssertFalse(inTransaction)

        for table in ["notes; DROP TABLE notes", "", "1notes", "no tes"] {
            do {
                try await database.insert(first, into: table)
                XCTFail("Accepted table name \(table)")
            } catch TinySQLError.invalidInput { }
        }
        let empty = try await database.insertAll([Note](), into: "notes")
        XCTAssertEqual(empty, 0)
        try await database.close()
    }

    func testColumnMatchingAndDecodingErrors() throws {
        struct Row: Decodable, Equatable { var userName: String; var loginCount: Int; var nickname: String? }
        let result = QueryResult(
            columns: ["USER_NAME", "login_count"], rows: [[.text("ada"), .integer(7)]])
        XCTAssertEqual(try result.decode(Row.self), [Row(userName: "ada", loginCount: 7, nickname: nil)])

        let wrongType = QueryResult(columns: ["userName", "loginCount"], rows: [[.text("ada"), .text("many")]])
        XCTAssertThrowsError(try wrongType.decode(Row.self)) { error in
            guard case DecodingError.typeMismatch = error else { return XCTFail("\(error)") }
        }
        let nullValue = QueryResult(columns: ["userName", "loginCount"], rows: [[.null, .integer(1)]])
        XCTAssertThrowsError(try nullValue.decode(Row.self)) { error in
            guard case DecodingError.valueNotFound = error else { return XCTFail("\(error)") }
        }
        let missing = QueryResult(columns: ["userName"], rows: [[.text("ada")]])
        XCTAssertThrowsError(try missing.decode(Row.self)) { error in
            guard case DecodingError.keyNotFound = error else { return XCTFail("\(error)") }
        }
        let overflow = QueryResult(columns: ["value"], rows: [[.integer(300)]])
        XCTAssertThrowsError(try overflow.decode(UInt8.self))
        XCTAssertEqual(try overflow.decode(Int16.self), [300])
        XCTAssertEqual(try QueryResult(columns: ["v"], rows: [[.real(3)]]).decode(Int.self), [3])
        XCTAssertThrowsError(try QueryResult(columns: ["v"], rows: [[.real(3.5)]]).decode(Int.self))
        XCTAssertThrowsError(try missing.decode([Int].self))
    }

    func testEncodingRejectsNonRecordsAndUnrepresentableValues() async throws {
        struct Big: Encodable, Sendable { var id: UInt64 }
        struct Bad: Encodable, Sendable { var ratio: Double }
        let database = try Database()
        try await database.execute("CREATE TABLE t (id INT)")
        do { try await database.insert(Big(id: UInt64.max), into: "t"); XCTFail("Overflow was accepted") }
        catch EncodingError.invalidValue { }
        do { try await database.insert(Bad(ratio: .nan), into: "t"); XCTFail("NaN was accepted") }
        catch EncodingError.invalidValue { }
        do { try await database.insert([1, 2, 3], into: "t"); XCTFail("An array is not a record") }
        catch EncodingError.invalidValue { }
        do { try await database.insert("text", into: "t"); XCTFail("A scalar is not a record") }
        catch EncodingError.invalidValue { }
        try await database.insert(Big(id: UInt64(Int64.max)), into: "t")
        let stored = try await database.query("SELECT id FROM t")
        XCTAssertEqual(stored.rows, [[.integer(Int64.max)]])
        try await database.close()
    }

    func testRawIntegerEnumsRespectSignedColumnRange() throws {
        enum Identifier: UInt64, Encodable {
            case valid = 9_223_372_036_854_775_807
            case overflow = 18_446_744_073_709_551_615
        }
        struct Record: Encodable { let id: Identifier }
        XCTAssertEqual(try RowEncoding.encode(Record(id: .valid)).values, [.integer(Int64.max)])
        XCTAssertThrowsError(try RowEncoding.encode(Record(id: .overflow))) { error in
            guard case EncodingError.invalidValue = error else { return XCTFail("\(error)") }
        }
    }

    func testTimestampFormatAndParse() throws {
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: 0)), .text("1970-01-01T00:00:00.000000Z"))
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: 1_700_000_000.25)), .text("2023-11-14T22:13:20.250000Z"))
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: -1.5)), .text("1969-12-31T23:59:58.500000Z"))
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: 951_782_400)), .text("2000-02-29T00:00:00.000000Z"))
        XCTAssertEqual(SQLValue.date(.distantFuture), .text("4001-01-01T00:00:00.000000Z"))
        XCTAssertEqual(SQLValue.date(.distantPast), .text("0001-01-01T00:00:00.000000Z"))
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: 253_402_300_799.5)), .text("9999-12-31T23:59:59.500000Z"))
        // Beyond four-digit years the value is clamped instead of overflowing.
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: 1e18)), .text("9999-12-31T23:59:59.000000Z"))
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: -1e18)), .text("0001-01-01T00:00:00.000000Z"))
        XCTAssertEqual(SQLValue.date(Date(timeIntervalSince1970: .nan)), .text("1970-01-01T00:00:00.000000Z"))

        let expectations: [(String, Double?)] = [
            ("2023-11-14T22:13:20Z", 1_700_000_000),
            ("2023-11-14T22:13:20.5Z", 1_700_000_000.5),
            ("2023-11-14T22:13:20.123456789Z", 1_700_000_000.123456789),
            ("2023-11-14t22:13:20z", 1_700_000_000),
            ("2023-11-14 22:13:20", 1_700_000_000),
            ("2023-11-15T00:13:20+02:00", 1_700_000_000),
            ("2023-11-14T17:13:20-0500", 1_700_000_000),
            ("2023-11-14T22:13Z", 1_699_999_980),
            ("2023-11-14", 1_699_920_000),
            ("2023-02-29", nil), ("2023-13-01", nil), ("2023-11-14T24:00:00Z", nil),
            ("2023-11-14T22:13:20", 1_700_000_000), ("2023-11-14T22:13:20.Z", nil),
            ("2023-11-14T22:13:20+25:00", nil), ("2023-11-14T22:13:20Zjunk", nil),
            ("yesterday", nil), ("", nil), ("2023-11-14T", nil),
        ]
        for (text, expected) in expectations {
            let parsed = SQLValue.text(text).dateValue?.timeIntervalSince1970
            if let expected, let parsed {
                XCTAssertEqual(parsed, expected, accuracy: 1e-6, text)
            } else {
                XCTAssertEqual(parsed == nil, expected == nil, text)
            }
        }
        XCTAssertEqual(SQLValue.integer(60).dateValue, Date(timeIntervalSince1970: 60))
        XCTAssertEqual(SQLValue.real(1.5).dateValue, Date(timeIntervalSince1970: 1.5))
        XCTAssertNil(SQLValue.blob(Data()).dateValue)

        // Text order is chronological order, which makes ORDER BY and BETWEEN work.
        var previous = ""
        for seconds in stride(from: -62_000_000_000.0, to: 253_000_000_000.0, by: 1_234_567_891.5) {
            let text = SQLTimestamp.format(Date(timeIntervalSince1970: seconds))
            XCTAssertGreaterThan(text, previous)
            previous = text
            XCTAssertEqual(SQLTimestamp.parse(text)?.timeIntervalSince1970 ?? .nan, seconds, accuracy: 1e-5)
        }
    }

    /// The "Typed models" example from the README, so the documentation stays true.
    func testReadmeTypedModelExample() async throws {
        struct Note: Codable, Sendable, Identifiable {
            var id: Int
            var body: String
            var createdAt: Date
            var pinned: Bool
            var tags: [String]
            var attachment: URL?
        }
        let database = try Database()
        try await database.execute("""
            CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body TEXT, created_at TIMESTAMP,
                                              pinned BOOL, tags TEXT, attachment TEXT)
            """)
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        try await database.insert(
            Note(id: 1, body: "Hello", createdAt: now, pinned: true, tags: ["a"], attachment: nil),
            into: "notes", columnNaming: .snakeCase)
        try await database.insert(
            Note(id: 2, body: "Later", createdAt: now + 60, pinned: false, tags: [], attachment: URL(string: "https://example.com/x")),
            into: "notes", columnNaming: .snakeCase)
        let pinned = try await database.query(
            "SELECT * FROM notes WHERE pinned = ? ORDER BY created_at DESC", parameters: [true], as: Note.self)
        XCTAssertEqual(pinned.map(\.id), [1])
        XCTAssertEqual(pinned.first?.tags, ["a"])
        XCTAssertNil(pinned.first?.attachment)
        let recent = try await database.query(
            "SELECT id FROM notes WHERE created_at > ? ORDER BY id", parameters: [.date(now + 1)], as: Int.self)
        XCTAssertEqual(recent, [2])
        let total = try await database.query("SELECT COUNT(*) FROM notes").decodeFirst(Int.self)
        XCTAssertEqual(total, 2)
        try await database.close()
    }

    func testColumnNamingAndCodingKeys() async throws {
        XCTAssertEqual(
            ["createdAt", "userID", "URLString", "id", "HTTPServer2Go", "already_snake", "x"].map(ColumnNaming.snakeCased),
            ["created_at", "user_id", "url_string", "id", "http_server2_go", "already_snake", "x"])

        struct Entry: Codable, Sendable, Equatable {
            var id: Int
            var displayName: String
            enum CodingKeys: String, CodingKey { case id, displayName = "display_name" }
        }
        let database = try Database()
        try await database.execute("CREATE TABLE entries (id INT, display_name TEXT)")
        try await database.insert(Entry(id: 1, displayName: "Ada"), into: "entries")
        let entries = try await database.query("SELECT * FROM entries", as: Entry.self)
        XCTAssertEqual(entries, [Entry(id: 1, displayName: "Ada")])
        try await database.close()
    }

    func testOptionalPropertiesInBatchesBecomeNull() async throws {
        struct Item: Codable, Sendable, Equatable { var id: Int; var label: String?; var weight: Double? }
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT PRIMARY KEY, label TEXT, weight FLOAT)")
        let items = [Item(id: 1, label: nil, weight: nil), Item(id: 2, label: "two", weight: nil), Item(id: 3, label: nil, weight: 3.5)]
        try await database.insertAll(items, into: "items")
        let loaded = try await database.query("SELECT * FROM items ORDER BY id", as: Item.self)
        XCTAssertEqual(loaded, items)
        try await database.insert(Item(id: 4, label: nil, weight: nil), into: "items")
        let all = try await database.query("SELECT id FROM items", as: Int.self)
        XCTAssertEqual(all.count, 4)
        try await database.close()
    }

    func testConvertiblesAndLiterals() {
        let values: [SQLValue] = [1, "two", 3.5, true, nil, SQLValue(Int8(4)), SQLValue(UInt32(5)),
                                  SQLValue(Optional<String>.none), SQLValue(Optional("x")), SQLValue(Data([1]))]
        XCTAssertEqual(values, [.integer(1), .text("two"), .real(3.5), .boolean(true), .null, .integer(4), .integer(5),
                                .null, .text("x"), .blob(Data([1]))])
        let id = UUID()
        XCTAssertEqual(SQLValue(id).uuidValue, id)
        XCTAssertEqual(SQLValue.url(URL(string: "https://example.com/a?b=c")!).urlValue?.absoluteString, "https://example.com/a?b=c")
        XCTAssertNil(SQLValue.text("not a uuid").uuidValue)
    }

    func testEngineAcceptsTimestampTextForTimestampColumns() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE events (id INT, at TIMESTAMP)")
        let moment = Date(timeIntervalSince1970: 1_700_000_000.5)
        try await database.execute("INSERT INTO events VALUES (?, ?)", parameters: [1, .date(moment)])
        let rows = try await database.query("SELECT at FROM events")
        XCTAssertEqual(rows.rows.first?.first?.dateValue?.timeIntervalSince1970 ?? .nan, 1_700_000_000.5, accuracy: 1e-3)
        try await database.close()
    }
}
