import Foundation

/// Values accepted as SQL parameters and returned in result rows.
/// Integers retain all 64 bits; BLOBs use Data, including embedded zero bytes.
public enum SQLValue: Sendable, Equatable, Codable {
    case null
    case integer(Int64)
    case real(Double)
    case text(String)
    case boolean(Bool)
    case blob(Data)

    public init(from decoder: Decoder) throws {
        let value = try decoder.singleValueContainer()
        if value.decodeNil() { self = .null }
        else if let bool = try? value.decode(Bool.self) { self = .boolean(bool) }
        else if let integer = try? value.decode(Int64.self) { self = .integer(integer) }
        else if let real = try? value.decode(Double.self) { self = .real(real) }
        else if let text = try? value.decode(String.self) { self = .text(text) }
        else {
            let object = try decoder.container(keyedBy: ValueKey.self)
            guard object.allKeys.count == 1, let key = object.allKeys.first else {
                throw DecodingError.dataCorruptedError(in: value, debugDescription: "Invalid SQL value")
            }
            switch key.stringValue {
            case "real":
                let real = try object.decode(Double.self, forKey: key)
                guard real.isFinite else {
                    throw DecodingError.dataCorruptedError(in: value, debugDescription: "Non-finite SQL real")
                }
                self = .real(real)
            case "blob":
                let encoded = try object.decode(String.self, forKey: key)
                guard let data = Data(base64Encoded: encoded) else {
                    throw DecodingError.dataCorruptedError(in: value, debugDescription: "Invalid SQL BLOB")
                }
                self = .blob(data)
            default:
                throw DecodingError.dataCorruptedError(in: value, debugDescription: "Unknown SQL value tag")
            }
        }
    }

    public func encode(to encoder: Encoder) throws {
        var value = encoder.singleValueContainer()
        switch self {
        case .null: try value.encodeNil()
        case .integer(let integer): try value.encode(integer)
        case .real(let real):
            guard real.isFinite else { throw TinySQLError.invalidInput("SQL numbers must be finite") }
            try value.encode(["real": real])
        case .text(let text): try value.encode(text)
        case .boolean(let bool): try value.encode(bool)
        case .blob(let data): try value.encode(["blob": data.base64EncodedString()])
        }
    }

    // Keep unknown keys visible so malformed objects cannot silently decode.
    private struct ValueKey: CodingKey {
        let stringValue: String
        let intValue: Int? = nil
        init?(stringValue: String) { self.stringValue = stringValue }
        init?(intValue: Int) { return nil }
    }
}

public struct QueryResult: Sendable, Equatable {
    public let columns: [String]
    /// Values in column order. Positional rows also preserve duplicate column names.
    public let rows: [[SQLValue]]
}

public enum TinySQLError: Error, Sendable, Equatable, LocalizedError {
    case closed
    case invalidInput(String)
    case database(String)
    case invalidResponse

    public var errorDescription: String? {
        switch self {
        case .closed: return "The tinySQL database is closed."
        case .invalidInput(let message), .database(let message): return message
        case .invalidResponse: return "The tinySQL bridge returned an invalid response."
        }
    }
}
