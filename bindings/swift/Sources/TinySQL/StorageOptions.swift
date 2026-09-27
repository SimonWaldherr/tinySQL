import Foundation

/// Durable storage backends for `Database(directory:storage:)`.
public enum StorageMode: String, Sendable, CaseIterable {
    /// In-memory tables with a write-ahead log and periodic checkpoints.
    case wal
    /// Row-level write-ahead log with checkpoints.
    case advancedWAL = "advanced_wal"
    /// One binary file per table.
    case disk
    /// One human-readable JSON file per table.
    case json
    /// Disk tables loaded on demand with a bounded cache.
    case index
    /// Disk tables with a bounded LRU cache.
    case hybrid
    /// Paged on-disk equality indexes for large read-mostly artifacts.
    case pagedIndex = "paged_index"
}

/// Durability barrier for WAL commits.
public enum WALSync: String, Sendable {
    /// The strongest flush the platform offers (default).
    case full
    /// A regular fsync; lower latency.
    case normal
}

/// Options for a durable database directory. Every acknowledged write is
/// persisted by the selected mode; `save(to:)` is not required.
public struct StorageOptions: Sendable, Equatable {
    public var mode: StorageMode
    /// Reject every write. The directory must already contain a database.
    public var readOnly: Bool
    /// Cache bound for `.index` and `.hybrid`; nil uses the engine default.
    public var maxMemoryBytes: Int64?
    public var walSync: WALSync
    /// 32-byte AES-256-GCM key for table files of `.disk`, `.json`, `.index`
    /// and `.hybrid`. Keep it in the Keychain, never next to the database.
    public var encryptionKey: Data?

    public init(
        mode: StorageMode = .wal,
        readOnly: Bool = false,
        maxMemoryBytes: Int64? = nil,
        walSync: WALSync = .full,
        encryptionKey: Data? = nil
    ) {
        self.mode = mode
        self.readOnly = readOnly
        self.maxMemoryBytes = maxMemoryBytes
        self.walSync = walSync
        self.encryptionKey = encryptionKey
    }

    /// The JSON object accepted by `TinySQLDatabaseOpenWithOptions`.
    func json(path: String) throws -> String {
        if let key = encryptionKey, key.count != 32 {
            throw TinySQLError.invalidInput("The encryption key must be exactly 32 bytes")
        }
        let payload = Payload(
            path: path,
            mode: mode.rawValue,
            readOnly: readOnly,
            maxMemoryBytes: maxMemoryBytes,
            walSync: walSync.rawValue,
            encryptionKey: encryptionKey?.base64EncodedString()
        )
        let data = try JSONEncoder().encode(payload)
        return String(decoding: data, as: UTF8.self)
    }

    // Optional members are omitted when nil; the bridge rejects unknown keys.
    private struct Payload: Encodable {
        let path: String
        let mode: String
        let readOnly: Bool
        let maxMemoryBytes: Int64?
        let walSync: String
        let encryptionKey: String?
    }
}
