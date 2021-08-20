use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::error::RdmaCmError;
use rdma_cm::{CommunicationManager, RdmaCmEvent};
use std::net::SocketAddr;
use std::ptr::null_mut;

#[test]
fn rdma_write() -> rdma_cm::Result<()> {
    utilities::run_server_client(server, client)
}

fn server(server_is_ready: Box<dyn Fn()>) -> Result<(), RdmaCmError> {
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
    let _qp = connected_id.create_qp(&pd, &cq);

    connected_id.accept()?;
    let event = listening_id.get_cm_event()?;
    assert_eq!(RdmaCmEvent::Established, event.get_event());
    event.ack();

    // Do nothing!

    let event = connected_id.get_cm_event()?;
    assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
    event.ack();
    connected_id.disconnect()?;
    Ok(())
}

fn client() -> Result<(), RdmaCmError> {
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

    cm_connection.connect()?;

    let event = cm_connection.get_cm_event()?;
    assert_eq!(RdmaCmEvent::Established, event.get_event());
    event.ack();

    cm_connection.disconnect()?;
    let event = cm_connection.get_cm_event()?;
    assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
    event.ack();
    Ok(())
}
