use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::error::RdmaCmError;
use rdma_cm::{CommunicationManager, PostSendOpcode, RdmaCmEvent, RegisteredMemory};
use std::net::SocketAddr;
use std::ptr::null_mut;
use tracing_subscriber::EnvFilter;
/// Connection data transmitted through the private data struct fields by our `connect` and `accept`
/// function to set up one-sided RDMA. (u64, u32) are (address of volatile_send_window, rkey).
#[derive(Clone, Copy, Debug)]
pub struct PeerConnectionData {
    remote_address: *mut u64,
    rkey: u32,
}

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

    let peer: PeerConnectionData = event.get_private_data().expect("Private data missing!");
    dbg!(peer);
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
    let work = vec![(1, memory)];

    let rdma_write = PostSendOpcode::WrRdmaWrite {
        rkey: peer.rkey,
        remote_address: peer.remote_address,
    };

    qp.post_send(work.iter(), rdma_write);

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

    let mut memory: RegisteredMemory<u64, 1> = pd.allocate_memory::<u64, 1>();

    let connection_data = PeerConnectionData {
        remote_address: memory.memory.as_mut_ptr(),
        rkey: memory.get_rkey(),
    };
    dbg!(connection_data);
    cm_connection.connect_with_data(&connection_data)?;

    let event = cm_connection.get_cm_event()?;
    assert_eq!(RdmaCmEvent::Established, event.get_event());
    event.ack();

    let ptr = memory.memory.as_ptr() as *const [u64; 1];
    unsafe {
        loop {
            let value: [u64; 1] = std::ptr::read_volatile(ptr);
            if value != [0] {
                println!("Server set value to: {:?}", value);
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
