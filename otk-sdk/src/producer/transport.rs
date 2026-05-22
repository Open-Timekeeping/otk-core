/// Transport endpoint for a producer connection.
///
/// Currently only TCP is supported. Serial port support is planned
/// (see `producer-serial` in the open questions section of the README).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    /// Plain TCP to the given address.
    Tcp(std::net::SocketAddr),
}
