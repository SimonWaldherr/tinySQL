import Foundation

// Foundation types as SQL values.
//
// Dates are stored as RFC 3339 text in UTC with microsecond precision
// ("2026-11-09T17:14:57.250000Z"). This is compatible with the engine's RFC 3339
// timestamps and sorts chronologically when used consistently in a TEXT column.
// UUIDs and URLs are stored as their canonical text, Data as BLOB.

/// A Swift value that has a natural SQL representation.
///
/// Integers wider than 63 bits (`UInt`, `UInt64`) intentionally do not conform:
/// they cannot be stored losslessly in a signed 64-bit column.
public protocol SQLValueConvertible: Sendable {
    var sqlValue: SQLValue { get }
}

extension SQLValue: SQLValueConvertible {
    public var sqlValue: SQLValue { self }
}

extension SQLValue {
    /// Wraps any convertible Swift value: `SQLValue(Date())`, `SQLValue(count)`.
    public init(_ value: some SQLValueConvertible) {
        self = value.sqlValue
    }

    public static func date(_ value: Date) -> SQLValue { .text(SQLTimestamp.format(value)) }
    public static func uuid(_ value: UUID) -> SQLValue { .text(value.uuidString) }
    public static func url(_ value: URL) -> SQLValue { .text(value.absoluteString) }

    /// A date parsed from RFC 3339 / ISO 8601 text (`2026-11-09T17:14:57Z`,
    /// fractional seconds and numeric offsets allowed, a date alone or a space
    /// instead of `T` too), or interpreted as seconds since 1970 for numbers.
    public var dateValue: Date? {
        switch self {
        case .text(let text): return SQLTimestamp.parse(text)
        case .integer(let seconds): return Date(timeIntervalSince1970: Double(seconds))
        case .real(let seconds): return Date(timeIntervalSince1970: seconds)
        default: return nil
        }
    }

    public var uuidValue: UUID? {
        if case .text(let text) = self { return UUID(uuidString: text) }
        return nil
    }

    public var urlValue: URL? {
        if case .text(let text) = self { return URL(string: text) }
        return nil
    }
}

extension SQLValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) { self = .null }
}

extension Int: SQLValueConvertible { public var sqlValue: SQLValue { .integer(Int64(self)) } }
extension Int8: SQLValueConvertible { public var sqlValue: SQLValue { .integer(Int64(self)) } }
extension Int16: SQLValueConvertible { public var sqlValue: SQLValue { .integer(Int64(self)) } }
extension Int32: SQLValueConvertible { public var sqlValue: SQLValue { .integer(Int64(self)) } }
extension Int64: SQLValueConvertible { public var sqlValue: SQLValue { .integer(self) } }
extension UInt8: SQLValueConvertible { public var sqlValue: SQLValue { .integer(Int64(self)) } }
extension UInt16: SQLValueConvertible { public var sqlValue: SQLValue { .integer(Int64(self)) } }
extension UInt32: SQLValueConvertible { public var sqlValue: SQLValue { .integer(Int64(self)) } }
extension Double: SQLValueConvertible { public var sqlValue: SQLValue { .real(self) } }
extension Float: SQLValueConvertible { public var sqlValue: SQLValue { .real(Double(self)) } }
extension Bool: SQLValueConvertible { public var sqlValue: SQLValue { .boolean(self) } }
extension String: SQLValueConvertible { public var sqlValue: SQLValue { .text(self) } }
extension Data: SQLValueConvertible { public var sqlValue: SQLValue { .blob(self) } }
extension Date: SQLValueConvertible { public var sqlValue: SQLValue { .date(self) } }
extension UUID: SQLValueConvertible { public var sqlValue: SQLValue { .uuid(self) } }
extension URL: SQLValueConvertible { public var sqlValue: SQLValue { .url(self) } }

extension Optional: SQLValueConvertible where Wrapped: SQLValueConvertible {
    public var sqlValue: SQLValue {
        switch self {
        case .none: return .null
        case .some(let value): return value.sqlValue
        }
    }
}

/// RFC 3339 timestamps without Foundation formatters: no shared mutable
/// formatter state, no locale or calendar influence, and cheap enough for bulk
/// imports.
enum SQLTimestamp {
    // 0001-01-01T00:00:00Z and 9999-12-31T23:59:59Z, the range four-digit years
    // can express. Anything outside is clamped instead of overflowing.
    private static let minimum = -62_135_596_800.0
    private static let maximum = 253_402_300_799.0

    static func format(_ date: Date) -> String {
        var seconds = date.timeIntervalSince1970
        if seconds.isNaN { seconds = 0 }
        // Fractions within the final valid second are still in range.
        seconds = max(seconds, minimum)
        if seconds >= maximum + 1 { seconds = maximum }
        var micros = Int64((seconds * 1_000_000).rounded())
        let top = Int64(maximum) * 1_000_000 + 999_999
        micros = min(micros, top)
        var whole = micros / 1_000_000
        var fraction = micros % 1_000_000
        if fraction < 0 { fraction += 1_000_000; whole -= 1 }
        let days = Int(floorDivide(whole, 86_400))
        let secondOfDay = Int(whole - Int64(days) * 86_400)
        let (year, month, day) = civil(fromDays: days)
        return "\(pad(year, 4))-\(pad(month, 2))-\(pad(day, 2))"
            + "T\(pad(secondOfDay / 3600, 2)):\(pad(secondOfDay / 60 % 60, 2)):\(pad(secondOfDay % 60, 2))"
            + ".\(pad(Int(fraction), 6))Z"
    }

    static func parse(_ text: String) -> Date? {
        let bytes = Array(text.utf8)
        var i = 0

        func digits(_ count: Int) -> Int? {
            guard i + count <= bytes.count else { return nil }
            var value = 0
            for offset in 0..<count {
                let c = bytes[i + offset]
                guard c >= 0x30 && c <= 0x39 else { return nil }
                value = value * 10 + Int(c - 0x30)
            }
            i += count
            return value
        }
        func skip(_ c: UInt8) -> Bool {
            guard i < bytes.count, bytes[i] == c else { return false }
            i += 1
            return true
        }

        guard let year = digits(4), skip(0x2D), let month = digits(2), skip(0x2D), let day = digits(2),
              (1...12).contains(month), (1...daysInMonth(year, month)).contains(day) else { return nil }
        var secondOfDay = 0
        var fraction = 0.0
        var offset = 0
        if i < bytes.count {
            guard skip(0x54) || skip(0x74) || skip(0x20) else { return nil }
            guard let hour = digits(2), skip(0x3A), let minute = digits(2),
                  hour < 24, minute < 60 else { return nil }
            var second = 0
            if skip(0x3A) {
                guard let s = digits(2), s < 61 else { return nil }
                second = s
            }
            secondOfDay = hour * 3600 + minute * 60 + second
            if skip(0x2E) {
                var scale = 0.1
                var seen = false
                while i < bytes.count, bytes[i] >= 0x30, bytes[i] <= 0x39 {
                    fraction += Double(bytes[i] - 0x30) * scale
                    scale /= 10
                    seen = true
                    i += 1
                }
                guard seen else { return nil }
            }
            if i < bytes.count {
                if skip(0x5A) || skip(0x7A) {
                    offset = 0
                } else if bytes[i] == 0x2B || bytes[i] == 0x2D {
                    let sign = bytes[i] == 0x2D ? -1 : 1
                    i += 1
                    guard let oh = digits(2), oh < 24 else { return nil }
                    _ = skip(0x3A)
                    guard let om = digits(2), om < 60 else { return nil }
                    offset = sign * (oh * 3600 + om * 60)
                } else {
                    return nil
                }
            }
            guard i == bytes.count else { return nil }
        }
        let days = daysFromCivil(year, month, day)
        let seconds = Double(days) * 86_400 + Double(secondOfDay - offset) + fraction
        return Date(timeIntervalSince1970: seconds)
    }

    // Howard Hinnant's civil-calendar algorithms (proleptic Gregorian).
    private static func daysFromCivil(_ year: Int, _ month: Int, _ day: Int) -> Int {
        let y = month <= 2 ? year - 1 : year
        let era = (y >= 0 ? y : y - 399) / 400
        let yearOfEra = y - era * 400
        let dayOfYear = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1
        let dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear
        return era * 146_097 + dayOfEra - 719_468
    }

    private static func civil(fromDays days: Int) -> (Int, Int, Int) {
        let z = days + 719_468
        let era = (z >= 0 ? z : z - 146_096) / 146_097
        let dayOfEra = z - era * 146_097
        let yearOfEra = (dayOfEra - dayOfEra / 1460 + dayOfEra / 36_524 - dayOfEra / 146_096) / 365
        let dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100)
        let mp = (5 * dayOfYear + 2) / 153
        let day = dayOfYear - (153 * mp + 2) / 5 + 1
        let month = mp < 10 ? mp + 3 : mp - 9
        return (yearOfEra + era * 400 + (month <= 2 ? 1 : 0), month, day)
    }

    private static func daysInMonth(_ year: Int, _ month: Int) -> Int {
        switch month {
        case 2: return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 ? 29 : 28
        case 4, 6, 9, 11: return 30
        default: return 31
        }
    }

    private static func floorDivide(_ a: Int64, _ b: Int64) -> Int64 {
        let q = a / b
        return (a % b != 0 && (a < 0) != (b < 0)) ? q - 1 : q
    }

    private static func pad(_ value: Int, _ width: Int) -> String {
        let text = String(value)
        return String(repeating: "0", count: max(0, width - text.count)) + text
    }
}
