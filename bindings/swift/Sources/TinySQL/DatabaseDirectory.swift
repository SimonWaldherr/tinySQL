import Foundation

/// How strongly the operating system protects files while the device is locked
/// (iOS, iPadOS). macOS ignores it.
public enum FileProtection: Sendable, Equatable {
    /// No protection class is set.
    case none
    /// Readable once the app has opened them, even after the device locks.
    case completeUnlessOpen
    /// Readable after the first unlock following a restart. This is what apps
    /// that work in the background (sync, notifications, widgets) need.
    case completeUntilFirstUserAuthentication
    /// Readable only while the device is unlocked. A background task that wakes
    /// the app on a locked device cannot open the database.
    case complete

    var foundationValue: FileProtectionType? {
        switch self {
        case .none: return FileProtectionType.none
        case .completeUnlessOpen: return .completeUnlessOpen
        case .completeUntilFirstUserAuthentication: return .completeUntilFirstUserAuthentication
        case .complete: return .complete
        }
    }
}

/// Places for a database directory that follow Apple's conventions for the
/// sandbox, App Groups, backups and Data Protection.
///
///     let directory = try DatabaseDirectory.applicationSupport("notes")
///     let database = try await Database.open(directory: directory)
///
/// Pass the result to `Database.open(directory:storage:)`. Each helper creates
/// the directory when it is missing.
public enum DatabaseDirectory {
    /// `<Application Support>/<app identifier>/<name>`. Sandboxed iOS and macOS apps
    /// already have a private Application Support folder; an unsandboxed Mac app
    /// shares one with every other app, which is why the bundle identifier is
    /// part of the path.
    ///
    /// - Parameters:
    ///   - fileProtection: Applied to the directory; files created inside inherit
    ///     it on iOS. The default keeps the database usable in the background.
    ///   - excludeFromBackup: Keep the database out of iCloud and iTunes backups,
    ///     for caches that can be rebuilt. Leave it off for user data.
    public static func applicationSupport(
        _ name: String = "Database",
        fileProtection: FileProtection = .completeUntilFirstUserAuthentication,
        excludeFromBackup: Bool = false
    ) throws -> URL {
        let base = try FileManager.default.url(
            for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
        let owner = Bundle.main.bundleIdentifier ?? ProcessInfo.processInfo.processName
        return try prepare(
            base.appendingPathComponent(owner, isDirectory: true).appendingPathComponent(name, isDirectory: true),
            fileProtection: fileProtection, excludeFromBackup: excludeFromBackup)
    }

    /// `<App Group container>/<name>`, so an app, its widgets and its extensions
    /// open the same database. The group must be listed in every target's
    /// entitlements; opening the same durable directory from several processes
    /// at once is not supported, so let one process own writes.
    public static func appGroup(
        _ identifier: String,
        _ name: String = "Database",
        fileProtection: FileProtection = .completeUntilFirstUserAuthentication,
        excludeFromBackup: Bool = false
    ) throws -> URL {
        guard let container = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: identifier) else {
            throw TinySQLError.invalidInput(
                "App Group \"\(identifier)\" is not available; add it to the target's entitlements")
        }
        do {
            return try prepare(
                container.appendingPathComponent(name, isDirectory: true),
                fileProtection: fileProtection, excludeFromBackup: excludeFromBackup)
        } catch {
            // macOS hands out a container URL for any group name and fails only
            // when the folder is created without the entitlement.
            throw TinySQLError.invalidInput(
                "App Group \"\(identifier)\" is not accessible (\(error.localizedDescription)); check the target's entitlements")
        }
    }

    /// Creates `url` (and missing parents) and applies the options to it.
    @discardableResult
    public static func prepare(
        _ url: URL,
        fileProtection: FileProtection = .completeUntilFirstUserAuthentication,
        excludeFromBackup: Bool = false
    ) throws -> URL {
        guard url.isFileURL, !url.path.isEmpty, !url.path.utf8.contains(0) else {
            throw TinySQLError.invalidInput("Expected a nonempty file URL without NUL")
        }
        let manager = FileManager.default
        try manager.createDirectory(at: url, withIntermediateDirectories: true)
        #if os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
        if let protection = fileProtection.foundationValue {
            try manager.setAttributes([.protectionKey: protection], ofItemAtPath: url.path)
        }
        #endif
        var directory = url
        var values = URLResourceValues()
        values.isExcludedFromBackup = excludeFromBackup
        try directory.setResourceValues(values)
        return url
    }
}

extension Database {
    /// Opens a durable database in the app's Application Support folder on a
    /// Dispatch worker; shorthand for `DatabaseDirectory.applicationSupport` plus
    /// `open(directory:storage:)`.
    public static func openInApplicationSupport(
        _ name: String = "Database",
        storage: StorageOptions = StorageOptions(),
        fileProtection: FileProtection = .completeUntilFirstUserAuthentication,
        excludeFromBackup: Bool = false
    ) async throws -> Database {
        let directory = try DatabaseDirectory.applicationSupport(
            name, fileProtection: fileProtection, excludeFromBackup: excludeFromBackup)
        return try await open(directory: directory, storage: storage)
    }
}
