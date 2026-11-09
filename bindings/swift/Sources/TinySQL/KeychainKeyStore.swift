import Foundation
import Security

/// A failed Keychain operation, carrying the `OSStatus` from Security.framework.
public struct KeychainError: Error, Sendable, Equatable, LocalizedError {
    public let status: OSStatus

    public var errorDescription: String? {
        let message = SecCopyErrorMessageString(status, nil) as String? ?? "unknown error"
        return "Keychain error \(status): \(message)"
    }
}

/// Stores the 32-byte key for `StorageOptions.encryptionKey` in the Keychain, so
/// it never sits next to the database it protects.
///
///     let key = try KeychainKeyStore().loadOrCreateKey()
///     let database = try await Database.open(
///         directory: directory, storage: StorageOptions(mode: .disk, encryptionKey: key))
///
/// The key is created from the system's secure random generator the first time
/// and read back afterwards. On iOS and the macOS data protection keychain it is
/// available after first unlock and does not migrate to a different device via
/// backup restore. The default macOS login keychain follows its own access and
/// backup policies. Keys are not synchronized through iCloud Keychain.
/// Losing the key loses the data.
public struct KeychainKeyStore: Sendable {
    public static let keyLength = 32

    public let service: String
    public let accessGroup: String?
    public let useDataProtectionKeychain: Bool

    /// - Parameters:
    ///   - service: Groups the app's items; use a name unique to your app.
    ///   - accessGroup: Share the key with other apps or extensions of your team.
    ///   - useDataProtectionKeychain: macOS only. `true` selects the iOS-style
    ///     keychain, which signed and sandboxed Mac apps with a keychain access
    ///     group can use; `false` uses the login keychain. iOS always uses the
    ///     data protection keychain.
    public init(
        service: String = "org.tinysql.encryption-key",
        accessGroup: String? = nil,
        useDataProtectionKeychain: Bool = false
    ) {
        self.service = service
        self.accessGroup = accessGroup
        self.useDataProtectionKeychain = useDataProtectionKeychain
    }

    /// The stored key for `account`, or nil when none exists.
    public func key(account: String = "default") throws -> Data? {
        var query = baseQuery(account)
        query[kSecReturnData as String] = true
        query[kSecMatchLimit as String] = kSecMatchLimitOne
        var item: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &item)
        switch status {
        case errSecSuccess:
            guard let data = item as? Data, data.count == Self.keyLength else {
                throw TinySQLError.invalidInput("The stored key for \"\(account)\" is not \(Self.keyLength) bytes")
            }
            return data
        case errSecItemNotFound:
            return nil
        default:
            throw KeychainError(status: status)
        }
    }

    /// Returns the key for `account`, creating and storing a random one first if
    /// there is none. Safe to call on every launch.
    public func loadOrCreateKey(account: String = "default") throws -> Data {
        if let existing = try key(account: account) { return existing }
        var bytes = [UInt8](repeating: 0, count: Self.keyLength)
        let random = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
        guard random == errSecSuccess else { throw KeychainError(status: random) }
        let key = Data(bytes)

        var attributes = baseQuery(account)
        attributes[kSecValueData as String] = key
        #if os(macOS)
        if useDataProtectionKeychain {
            attributes[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        }
        #else
        attributes[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        #endif
        let status = SecItemAdd(attributes as CFDictionary, nil)
        if status == errSecDuplicateItem, let winner = try self.key(account: account) {
            return winner  // another process or task created it between the read and the add
        }
        guard status == errSecSuccess else { throw KeychainError(status: status) }
        return key
    }

    /// Removes the key for `account`. Data encrypted with it can no longer be read.
    public func deleteKey(account: String = "default") throws {
        let status = SecItemDelete(baseQuery(account) as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw KeychainError(status: status) }
    }

    private func baseQuery(_ account: String) -> [String: Any] {
        var query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
        ]
        if let accessGroup { query[kSecAttrAccessGroup as String] = accessGroup }
        #if os(macOS)
        if useDataProtectionKeychain { query[kSecUseDataProtectionKeychain as String] = true }
        #endif
        return query
    }
}
