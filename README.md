# rdma-cm

High-level Rust bindings around RDMA-cm. We also include wrappers around ibverbs methods.f

# Dependencies

This crate requires the following external dependencies:
- **libibverbs-dev**: RDMA library for Infiniband verbs.
- **librdmacm-dev**: RDMA Communication Manager: Technology agnostic communication layer for RDMA connections.
- **libclang-dev**: To generate RDMA Rust bindings around ibverbs and rdma_cm headers.


### Why not Rust-ibverbs?
The Rust [ibverbs](https://github.com/jonhoo/rust-ibverbs) crate already provides high quality, high level bindings to ibverbs.
However, it doesn't provide the communication mechanism for connecting two nodes. This makes ibverbs connection agnostic: users
should share the ibverbs::QueuePairEndpoint between the two nodes to connect the QueuePair.

However, RDMA-cm handles the queue pair connection returning an initialized QueuePair. Currently the ibverbs crate does not accept an already
initialized queue pair, verbs context, protection domain, etc. Furthermore, the ibverbs crate only allows allocation/registration of dynamically
allocated buffers owned by the crate. At this point it is unclear if we need more flexibility.


In the future we may attempt integrating with this crate to avoid duplication.
