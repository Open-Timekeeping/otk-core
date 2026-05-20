use minicbor::{Decode, Encode};

/// How a timestamp was produced. Honest reporting is required; the consumer decides
/// whether a given method meets their precision requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimestampingMethod {
    /// Timer peripheral captured the signal edge in silicon, independent of CPU activity.
    /// Highest trust; jitter bounded by the timer clock, not interrupt latency.
    #[n(0)]
    HardwareEventCapture,

    /// CPU read the clock in an interrupt service routine after the signal fired.
    /// Subject to interrupt latency (typically 1-50 µs on bare-metal MCU).
    #[n(1)]
    FirmwareTimerRead,

    /// Timestamp assigned by the adapter process when it received the data from the device.
    /// Always later than the physical event; useful for latency analysis only.
    #[n(2)]
    AdapterReceiveTime,

    /// Timestamp entered by a human operator.
    #[n(3)]
    ManualEntry,

    /// Timestamp is from an original recorded event being replayed.
    #[n(4)]
    ReplayRecorded,
}

/// How the timebase identity carried in an event was determined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SourceAttestation {
    /// Identity was read from the sync source at runtime (PTP grandmaster Clock-ID,
    /// GNSS constellation, NTP refid chain). Hardware-grounded.
    #[n(0)]
    RuntimeDiscovered,

    /// Identity comes from operator configuration only (e.g. PPS-over-coax where the
    /// detector sees a pulse with no identifying metadata).
    #[n(1)]
    OperatorAsserted,
}

/// Sync state of a timebase at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SyncState {
    /// Actively disciplined against the reference; within normal uncertainty bounds.
    #[n(0)]
    Locked,

    /// Reference lost; running on stored discipline. Uncertainty growing over time.
    #[n(1)]
    Holdover,

    /// Never successfully synced or discipline fully expired; free-running local clock.
    #[n(2)]
    FreeRun,

    /// Explicitly unsynchronized (e.g. reference configured but unreachable).
    #[n(3)]
    Unsynchronized,

    /// Sync state cannot be determined.
    #[n(4)]
    Unknown,
}
