# rdma-cm

Idiomatic Rust bindings around RDMA CM (Communication Manager) and ibverbs. We also expose the raw rust-bindgen bindings
around ibverbs and rdmacm via the `ffi` module.

Since RDMA CM is only useful with ibverbs. This crate provides seamless interoperability between the two.

This crate aims to provide "zero cost" high-level bindings for rdma-cm. Providing a far better API than the C RDMA CM
bindings, but avoiding the heavy cost that may be associated with such abstractions. E.g, our completion queue is just
a wrapper around the underlying ibv_cq pointer:
```rust
struct CompletionQueue<const POLL_ELEMENTS: usize> {
    cq: *mut ffi::ibv_cq,
}
```
# Dependencies
This crate requires the following external dependencies:
- **libibverbs-dev**: RDMA library for Infiniband verbs.
- **librdmacm-dev**: RDMA Communication Manager: Technology agnostic communication layer for RDMA connections.
- **libclang-dev**: To generate RDMA Rust bindings around ibverbs and rdma_cm headers.


### Why not Rust-ibverbs?
The Rust [ibverbs](https://github.com/jonhoo/rust-ibverbs) crate already provides high quality, high level bindings to
ibverbs. However, it doesn't provide the communication mechanism for connecting two endpoints: It is up to you to share
`ibverbs::QueuePairEndpoint` between the two nodes to connect the QueuePair. For example, by using a TCP connection.

RDMA CM is designed to connect two RDMA endpoints while being protocol agnostic. 

However, RDMA CM handles the queue pair connection by returning an initialized QueuePair. Currently, the ibverbs crate
does not accept an already initialized queue pair, verbs context, protection domain, etc. Furthermore, the ibverbs crate
only allows allocation/registration of dynamically allocated buffers owned by the crate. This may not be flexible enough
in all use cases.
