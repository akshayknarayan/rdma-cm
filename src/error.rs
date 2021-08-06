use thiserror::Error;

pub type Result<T> = std::result::Result<T, RdmaCmError>;

// This isn't ideal instead of having an enum variant per action, we should have have
// actions return the underlying reason. Unfortunately, RDMA cm and ibverbs don't seem
// to enumerate anywhere all the errors that a function could return. A la linux man
// pages.
#[derive(Error, Debug)]
pub enum RdmaCmError {
    /// ibverbs seems to provide no underlying reason of why this would fail.
    #[error("Cannot allocate protection domain.")]
    ProtectionDomain,
    #[error("Cannot create completion queue.")]
    CreateCompletionQueue(std::io::Error),
    #[error("Cannot fetch next CM event.")]
    GetCmEvent(std::io::Error),
    #[error("Unable to resolve route.")]
    ResolveRoute(std::io::Error),
    #[error("Unable to resolve address.")]
    ResolveAddress(std::io::Error),
    #[error("Unable to get address info.")]
    GetAddressInfo(std::io::Error),
    #[error("Unable to listen.")]
    Listen(std::io::Error),
    #[error("Unable to bind to address.")]
    Bind(std::io::Error),
    #[error("Unable to accept connection.")]
    Accept(std::io::Error),
    #[error("Unable to connect.")]
    Connect(std::io::Error),
    #[error("Unable to create RDMA event channel.")]
    RdmaEventChannel(std::io::Error),
    #[error("Unable to create RDMA Device ID.")]
    RdmaCreateId(std::io::Error),
    #[error("Unable to disconnect RDMA connction.")]
    Disconnect(std::io::Error),
}
