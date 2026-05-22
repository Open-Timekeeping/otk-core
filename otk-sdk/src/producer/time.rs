/// Current wall-clock time as nanoseconds since Unix epoch.
///
/// Panics if the system clock reports a time before the Unix epoch (misconfigured
/// system clock) or past year 2554 (u64 overflow after ~584 years from epoch).
pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is set before the Unix epoch")
        .as_nanos()
        .try_into()
        .expect("timestamp overflows u64; system clock reports a date past year 2554")
}
