import Dispatch

/// Native database calls can block on Go or file I/O. Keep them off Swift's
/// cooperative executor while retaining actor isolation and serial execution.
final class DatabaseExecutor: SerialExecutor {
    private let queue = DispatchQueue(label: "org.tinysql.database", qos: .utility)

    // Required for the package's macOS 12 / iOS 15 deployment targets.
    func enqueue(_ job: UnownedJob) {
        queue.async {
            job.runSynchronously(on: self.asUnownedSerialExecutor())
        }
    }

    func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        UnownedSerialExecutor(ordinary: self)
    }
}
