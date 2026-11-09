import Foundation
import XCTest
@testable import TinySQL

private struct Item: Codable, Sendable, Equatable { var id: Int; var name: String }

final class ObservationTests: XCTestCase {
    func testSlowObserverKeepsOnlyTheLatestSnapshot() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT)")
        let updates = (1...4).map { expectation(description: "Observed \($0) rows") }
        let stream = database.observeMapped("SELECT COUNT(*) FROM items", parameters: []) { result in
            let count = try result.decodeFirst(Int.self) ?? -1
            if (1...4).contains(count) { updates[count - 1].fulfill() }
            return count
        }
        var iterator = stream.makeAsyncIterator()
        let initial = try await iterator.next()
        XCTAssertEqual(initial, 0)
        for count in 1...4 {
            try await database.execute("INSERT INTO items VALUES (?)", parameters: [SQLValue(count)])
            await fulfillment(of: [updates[count - 1]], timeout: 2)
        }
        // Pause consumption while the producer publishes the last snapshot.
        try await settle()
        let latest = try await iterator.next()
        XCTAssertEqual(latest, 4, "A slow consumer must not retain obsolete result snapshots")
        try await database.close()
    }

    private actor Counter {
        private(set) var value = 0
        func increment() { value += 1 }
    }

    /// Counts elements on a dedicated task. A cancelled `next()` would drop the
    /// iterator and end the stream, so "nothing arrived" is observed by counting.
    private func count(_ stream: AsyncStream<Void>) -> (Counter, Task<Void, Never>) {
        let counter = Counter()
        return (counter, Task { for await _ in stream { await counter.increment() } })
    }

    private func settle() async throws { try await Task.sleep(nanoseconds: 100_000_000) }

    func testChangesFireAfterSettledWritesOnly() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
        let changes = await database.changes()
        var iterator = changes.makeAsyncIterator()

        try await database.execute("INSERT INTO items VALUES (1, 'a')")
        _ = await iterator.next()

        // Nothing is published while a transaction is open, and reads never publish.
        try await database.execute("BEGIN")
        try await database.execute("INSERT INTO items VALUES (2, 'b')")
        _ = try await database.query("SELECT * FROM items")
        try await database.execute("COMMIT")
        _ = await iterator.next()

        // Writes that coalesce into one pending element still deliver at least one.
        try await database.executeBatch("INSERT INTO items VALUES (?, ?)", parameterSets: [[3, "c"], [4, "d"]])
        try await database.executeScript("INSERT INTO items VALUES (5, 'e'); INSERT INTO items VALUES (6, 'f')")
        _ = await iterator.next()

        // A failing statement may still have changed state in a script, so it publishes.
        _ = try? await database.executeScript("INSERT INTO items VALUES (7, 'g'); INSERT INTO items VALUES (7, 'dup')")
        _ = await iterator.next()

        // A transaction scope publishes once, after commit.
        try await database.transaction { db in
            try db.execute("INSERT INTO items VALUES (8, 'h')")
            try db.execute("INSERT INTO items VALUES (9, 'i')")
        }
        _ = await iterator.next()

        try await database.close()
        let end: Void? = await iterator.next()
        XCTAssertNil(end, "The stream must finish when the database closes")

        let closed = await database.changes()
        var closedIterator = closed.makeAsyncIterator()
        let none: Void? = await closedIterator.next()
        XCTAssertNil(none)
    }

    func testNoNotificationWhileTransactionIsOpenOrForReads() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT, name TEXT)")
        let (counter, listener) = count(await database.changes())
        defer { listener.cancel() }

        _ = try await database.query("SELECT * FROM items")
        try await settle()
        var seen = await counter.value
        XCTAssertEqual(seen, 0, "Reads must not notify")

        try await database.execute("BEGIN")
        try await database.execute("INSERT INTO items VALUES (1, 'a')")
        try await settle()
        seen = await counter.value
        XCTAssertEqual(seen, 0, "A write inside an open transaction must not notify yet")

        try await database.execute("ROLLBACK")
        try await settle()
        seen = await counter.value
        XCTAssertEqual(seen, 1, "Ending the transaction publishes exactly once")

        try await database.execute("INSERT INTO items VALUES (2, 'b')")
        try await settle()
        seen = await counter.value
        XCTAssertEqual(seen, 2)
        try await database.close()
        await listener.value
    }

    func testObserveYieldsCurrentResultThenUpdatesAndSkipsUnchanged() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
        try await database.execute("CREATE TABLE other (id INT)")
        try await database.execute("INSERT INTO items VALUES (1, 'a')")

        var iterator = database.observe("SELECT * FROM items ORDER BY id", as: Item.self).makeAsyncIterator()
        let initial = try await iterator.next()
        XCTAssertEqual(initial, [Item(id: 1, name: "a")])

        // A write to an unrelated table re-runs the query but yields nothing new, so
        // the next element is the one produced by the following relevant write.
        try await database.execute("INSERT INTO other VALUES (1)")
        try await database.execute("INSERT INTO items VALUES (2, 'b')")
        let updated = try await iterator.next()
        XCTAssertEqual(updated, [Item(id: 1, name: "a"), Item(id: 2, name: "b")])

        try await database.close()
        let end = try await iterator.next()
        XCTAssertNil(end)
    }

    func testObserveReportsQueryFailureAndStopsWhenConsumerLeaves() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT)")
        var iterator = database.observe("SELECT * FROM items").makeAsyncIterator()
        _ = try await iterator.next()
        try await database.execute("DROP TABLE items")
        do {
            _ = try await iterator.next()
            XCTFail("A dropped table must surface as an error")
        } catch TinySQLError.database { }

        var observers = 0
        do {
            let stream = database.observe("SELECT 1")
            var local = stream.makeAsyncIterator()
            _ = try await local.next()
            observers += 1
        }
        // Dropped streams unregister; closing afterwards must not hang or crash.
        try await Task.sleep(nanoseconds: 50_000_000)
        try await database.close()
        XCTAssertEqual(observers, 1)
    }

    @MainActor
    func testLiveQueryFollowsWrites() async throws {
        let database = try Database()
        try await database.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
        let live = LiveQuery<Item>(database: database, sql: "SELECT * FROM items ORDER BY id")
        XCTAssertFalse(live.hasLoaded)

        let running = Task { await live.run() }
        try await waitUntil { live.hasLoaded }
        XCTAssertEqual(live.rows, [])

        try await database.insert(Item(id: 1, name: "first"), into: "items")
        try await waitUntil { live.rows.count == 1 }
        try await database.insertAll([Item(id: 2, name: "second"), Item(id: 3, name: "third")], into: "items")
        try await waitUntil { live.rows.count == 3 }
        XCTAssertEqual(live.rows.map(\.name), ["first", "second", "third"])
        XCTAssertNil(live.error)

        running.cancel()
        await running.value
        try await database.insert(Item(id: 4, name: "fourth"), into: "items")
        try await Task.sleep(nanoseconds: 50_000_000)
        XCTAssertEqual(live.rows.count, 3, "A cancelled run must stop following changes")

        let failing = LiveQuery<Item>(database: database, sql: "SELECT * FROM missing_table")
        await failing.run()
        XCTAssertNotNil(failing.error)
        XCTAssertFalse(failing.hasLoaded)
        try await database.close()
    }

    @MainActor
    private func waitUntil(_ condition: @MainActor () -> Bool, file: StaticString = #filePath, line: UInt = #line) async throws {
        for _ in 0..<200 {
            if condition() { return }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTFail("Condition not reached within 2 s", file: file, line: line)
    }
}
