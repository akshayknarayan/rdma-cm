use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::error::RdmaCmError;
use rdma_cm::{
    CommunicationManager, PeerConnectionData, RdmaCmEvent, RdmaMemory, VolatileRdmaMemory,
};
use std::net::SocketAddr;
use std::ptr::null_mut;
use tracing_subscriber::EnvFilter;

#[test]
fn rdma_write() -> rdma_cm::Result<()> {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .without_time()
        .init();

    utilities::run_server_client(rdma_write_server, rdma_write_client)
}

fn rdma_write_server(server_is_ready: Box<dyn Fn()>) -> Result<(), RdmaCmError> {
    let listening_id = CommunicationManager::new()?;

    // Bind to local host.
    let address: SocketAddr = "127.0.0.1:4000"
        .parse()
        .expect("Unable to parse socket address");

    listening_id.bind(&SockAddr::Inet(InetAddr::from_std(&address)))?;
    listening_id.listen()?;
    server_is_ready();

    let event = listening_id.get_cm_event()?;
    assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());

    let connected_id = event.get_connection_request_id();

    let peer: PeerConnectionData<u64, 1> = event.get_private_data().expect("Private data missing!");
    event.ack();

    let pd = connected_id.allocate_protection_domain()?;
    let cq = connected_id.create_cq::<100>()?;
    let mut qp = connected_id.create_qp(&pd, &cq);

    connected_id.accept()?;
    let event = listening_id.get_cm_event()?;
    assert_eq!(RdmaCmEvent::Established, event.get_event());
    event.ack();

    let mut memory: RdmaMemory<u64, 1> = pd.allocate_memory::<u64, 1>();
    memory.as_mut_slice(1)[0] = 42;
    let work = vec![(1, memory)];

    qp.post_send(work.iter(), peer.as_rdma_write());

    loop {
        if let Some(mut entries) = cq.poll() {
            assert_eq!(entries.len(), 1);
            let e = entries.next().unwrap();
            assert_eq!(1, e.wr_id, "Incorrect work request id.");
            assert_eq!(e.status, 0, "Other completion status found.");
            break;
        }
    }

    let event = connected_id.get_cm_event()?;
    assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
    event.ack();
    connected_id.disconnect()?;

    Ok(())
}

fn rdma_write_client() -> Result<(), RdmaCmError> {
    let cm_connection = CommunicationManager::new()?;
    let addr_info = CommunicationManager::get_address_info("192.168.1.2", "4000")?;

    unsafe {
        let mut current = addr_info;

        while current != null_mut() {
            if let Ok(_) = cm_connection.resolve_address((*current).ai_dst_addr) {
                break;
            }

            current = (*current).ai_next;
        }
    }

    let event = cm_connection.get_cm_event()?;
    assert_eq!(RdmaCmEvent::AddressResolved, event.get_event());
    event.ack();

    cm_connection.resolve_route(1)?;
    let event = cm_connection.get_cm_event()?;
    assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
    event.ack();

    let pd = cm_connection.allocate_protection_domain()?;
    let cq = cm_connection.create_cq::<100>()?;
    let _qp = cm_connection.create_qp(&pd, &cq);

    let mut memory = VolatileRdmaMemory::<u64, 1>::new(&pd);
    cm_connection.connect_with_data(&memory.as_connection_data())?;

    let event = cm_connection.get_cm_event()?;
    assert_eq!(RdmaCmEvent::Established, event.get_event());
    event.ack();

    loop {
        let value = memory.read();
        if value != [0] {
            println!("Server set value to: {:?}", value);
            break;
        }
    }

    cm_connection.disconnect()?;
    let event = cm_connection.get_cm_event()?;
    assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);

    event.ack();
    Ok(())
}
