import Foundation
import Security
import XCTest
@testable import TinySQL

final class PlatformTests: XCTestCase {
    private func temporaryDirectory() -> URL {
        FileManager.default.temporaryDirectory.appendingPathComponent("tinysql-\(UUID().uuidString)", isDirectory: true)
    }

    func testPrepareCreatesNestedDirectoryAndAppliesBackupFlag() throws {
        let root = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }
        let target = root.appendingPathComponent("a/b/db", isDirectory: true)

        let prepared = try DatabaseDirectory.prepare(target, excludeFromBackup: true)
        XCTAssertEqual(prepared, target)
        var isDirectory: ObjCBool = false
        XCTAssertTrue(FileManager.default.fileExists(atPath: target.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
        XCTAssertEqual(try target.resourceValues(forKeys: [.isExcludedFromBackupKey]).isExcludedFromBackup, true)

        // Preparing again is harmless and can lift the exclusion.
        try DatabaseDirectory.prepare(target, excludeFromBackup: false)
        XCTAssertEqual(try target.resourceValues(forKeys: [.isExcludedFromBackupKey]).isExcludedFromBackup, false)

        XCTAssertThrowsError(try DatabaseDirectory.prepare(URL(string: "https://example.com/db")!))
    }

    func testDurableDatabaseInPreparedDirectoryReopens() async throws {
        let root = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }
        let directory = try DatabaseDirectory.prepare(root.appendingPathComponent("notes", isDirectory: true))

        struct Note: Codable, Sendable, Equatable { var id: Int; var body: String }
        let database = try await Database.open(directory: directory)
        try await database.execute("CREATE TABLE notes (id INT, body TEXT)")
        try await database.insert(Note(id: 1, body: "kept"), into: "notes")
        try await database.close()

        let reopened = try await Database.open(directory: directory)
        let notes = try await reopened.query("SELECT * FROM notes", as: Note.self)
        XCTAssertEqual(notes, [Note(id: 1, body: "kept")])
        try await reopened.close()

        // The default log-based mode keeps its file inside the prepared directory.
        let names = try FileManager.default.contentsOfDirectory(atPath: directory.path)
        XCTAssertTrue(names.contains(StorageOptions.logDatabaseFileName), "\(names)")

        // A path that does not exist yet still becomes the database file itself.
        let file = root.appendingPathComponent("plain.db")
        let plain = try await Database.open(directory: file)
        try await plain.execute("CREATE TABLE t (id INT)")
        try await plain.close()
        var isDirectory: ObjCBool = false
        _ = FileManager.default.fileExists(atPath: file.path, isDirectory: &isDirectory)
        XCTAssertFalse(isDirectory.boolValue, "The URL must not have been turned into a directory")
        let again = try await Database.open(directory: file)
        let tables = try await again.query("SELECT * FROM t")
        XCTAssertEqual(tables.columns, ["id"])
        try await again.close()
    }

    func testApplicationSupportDirectoryIsPerApp() throws {
        let url = try DatabaseDirectory.applicationSupport("tinysql-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: url) }
        XCTAssertTrue(FileManager.default.fileExists(atPath: url.path))
        XCTAssertTrue(url.path.contains("Application Support"))
        let owner = Bundle.main.bundleIdentifier ?? ProcessInfo.processInfo.processName
        XCTAssertTrue(url.deletingLastPathComponent().lastPathComponent == owner)
    }

    func testAppGroupWithoutEntitlementIsAClearError() throws {
        do {
            let url = try DatabaseDirectory.appGroup("group.org.tinysql.tests.\(UUID().uuidString.lowercased())")
            // The iOS Simulator and unsandboxed processes may hand out any group.
            try? FileManager.default.removeItem(at: url)
            throw XCTSkip("This environment does not enforce App Group entitlements")
        } catch TinySQLError.invalidInput(let message) {
            XCTAssertTrue(message.contains("entitlements"), message)
        }
    }

    /// Package tests run without a host app, so iOS refuses Keychain access with
    /// errSecMissingEntitlement; those environments skip instead of failing.
    private func skipWithoutKeychain(_ body: () throws -> Void) throws {
        do {
            try body()
        } catch let error as KeychainError where [
            errSecInteractionNotAllowed, errSecNotAvailable, errSecMissingEntitlement, errSecAuthFailed, errSecNoSuchKeychain,
        ].contains(error.status) {
            throw XCTSkip("No usable keychain in this environment: \(error.localizedDescription)")
        }
    }

    func testKeychainKeyLifecycle() throws {
        let store = KeychainKeyStore(service: "org.tinysql.tests.\(UUID().uuidString)")
        defer {
            try? store.deleteKey(account: "primary")
            try? store.deleteKey(account: "other")
        }
        try skipWithoutKeychain {
            let initial = try store.key(account: "primary")
            let created = try store.loadOrCreateKey(account: "primary")
            let again = try store.loadOrCreateKey(account: "primary")
            let read = try store.key(account: "primary")
            let other = try store.loadOrCreateKey(account: "other")
            try store.deleteKey(account: "other")
            try store.deleteKey(account: "primary")
            let afterDelete = try store.key(account: "primary")
            try store.deleteKey(account: "primary")

            XCTAssertNil(initial)
            XCTAssertEqual(created.count, KeychainKeyStore.keyLength)
            XCTAssertEqual(again, created, "The key must be stable across launches")
            XCTAssertEqual(read, created)
            XCTAssertNotEqual(other, created)
            XCTAssertNil(afterDelete)
        }
    }

    func testKeychainKeyEncryptsDatabaseFiles() async throws {
        let store = KeychainKeyStore(service: "org.tinysql.tests.\(UUID().uuidString)")
        defer { try? store.deleteKey() }
        var stored: Data?
        try skipWithoutKeychain { stored = try store.loadOrCreateKey() }
        let key = try XCTUnwrap(stored)
        let root = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: root) }
        let directory = try DatabaseDirectory.prepare(root)
        let options = StorageOptions(mode: .disk, encryptionKey: key)

        let database = try await Database.open(directory: directory, storage: options)
        try await database.execute("CREATE TABLE secrets (id INT, body TEXT)")
        try await database.execute("INSERT INTO secrets VALUES (1, 'needle-in-the-haystack')")
        try await database.close()

        let files = try FileManager.default.subpathsOfDirectory(atPath: directory.path)
        for file in files {
            let url = directory.appendingPathComponent(file)
            guard let data = try? Data(contentsOf: url) else { continue }
            XCTAssertNil(data.range(of: Data("needle-in-the-haystack".utf8)), "\(file) holds plaintext")
        }
        let reopened = try await Database.open(directory: directory, storage: options)
        let rows = try await reopened.query("SELECT body FROM secrets")
        XCTAssertEqual(rows.rows, [[.text("needle-in-the-haystack")]])
        try await reopened.close()
    }

    func testPlainTextEncryptionKeyLengthIsChecked() async {
        do {
            _ = try await Database.open(
                directory: temporaryDirectory(), storage: StorageOptions(mode: .disk, encryptionKey: Data(count: 16)))
            XCTFail("A 16-byte key was accepted")
        } catch TinySQLError.invalidInput { } catch { XCTFail("\(error)") }
    }
}
