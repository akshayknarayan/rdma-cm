use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::error::RdmaCmError;
use rdma_cm::{CommunicationManager, PostSendOpcode, RdmaCmEvent, RegisteredMemory};
use std::net::SocketAddr;
use std::ptr::null_mut;

#[test]
fn two_sided_rdma_send_byte() -> rdma_cm::Result<()> {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .without_time()
        .init();

    utilities::run_server_client(rdma_send_byte_server, rdma_send_byte_client)
}

fn rdma_send_byte_server(server_is_ready: Box<dyn Fn()>) -> Result<(), RdmaCmError> {
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
    event.ack();

    let pd = connected_id.allocate_protection_domain()?;
    let cq = connected_id.create_cq::<100>()?;
    let mut qp = connected_id.create_qp(&pd, &cq);

    connected_id.accept()?;
    let event = listening_id.get_cm_event()?;
    assert_eq!(RdmaCmEvent::Established, event.get_event());
    event.ack();

    let mut memory: RegisteredMemory<u64, 1> = pd.allocate_memory::<u64, 1>();
    memory.as_mut_slice(1)[0] = 42;
    let mut work = vec![(2, memory)];

    qp.post_receive(work.iter());
    let mut done: bool = false;
    while !done {
        if let Some(entries) = cq.poll() {
            for e in entries {
                assert_eq!(2, e.wr_id, "Incorrect work request id.");
                assert_eq!(e.status, 0, "Other completion status found.");
                println!("{:?}", work[0].1.as_mut_slice(1));
                done = true;
                break;
            }
        }
    }

    let event = connected_id.get_cm_event()?;
    assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
    event.ack();
    connected_id.disconnect()?;
    Ok(())
}

fn rdma_send_byte_client() -> Result<(), RdmaCmError> {
    let cm_connection = CommunicationManager::new()?;
    let addr_info = CommunicationManager::get_address_info(&"192.168.1.2", &"4000")?;

    unsafe {
        let mut current = addr_info;

        while current != null_mut() {
            if let Ok(_) = cm_connection.resolve_address((*current).ai_dst_addr) {
                println!("Client: Address resolved.");
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
    let mut qp = cm_connection.create_qp(&pd, &cq);
    cm_connection.connect()?;

    let event = cm_connection.get_cm_event()?;
    assert_eq!(RdmaCmEvent::Established, event.get_event());
    event.ack();

    let mut memory = pd.allocate_memory::<u64, 1>();
    memory.as_mut_slice(1)[0] = 42;

    let work = vec![(1, memory)];
    qp.post_send(work.iter(), PostSendOpcode::Send);

    let mut done = false;
    while !done {
        if let Some(entries) = cq.poll() {
            for e in entries {
                assert_eq!(1, e.wr_id, "Incorrect work request id.");
                assert_eq!(e.status, 0, "Other completion status found.");
                done = true;
                break;
            }
        }
    }

    cm_connection.disconnect()?;
    let event = cm_connection.get_cm_event()?;
    assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
    event.ack();
    Ok(())
}
