use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::error::RdmaCmError;
use rdma_cm::{CommunicationManager, PostSendOpcode, RdmaCmEvent, RegisteredMemory};
use std::net::SocketAddr;
use std::ptr::null_mut;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "RMDA CM Client/Server",
    about = "Example RDMA CM Client/Server Program."
)]
enum Mode {
    Client {
        // IP address to connect to.
        #[structopt(long)]
        ip_address: String,
        // Port to connect to.
        #[structopt(short, long)]
        port: u16,
    },
    Server {
        // Port to listen on.
        #[structopt(short, long)]
        port: u16,
    },
}

/// Connection data transmitted through the private data struct fields by our `connect` and `accept`
/// function to set up one-sided RDMA. (u64, u32) are (address of volatile_send_window, rkey).
#[derive(Clone, Copy, Debug)]
pub struct PeerConnectionData {
    remote_address: *mut u64,
    rkey: u32,
}

fn main() -> Result<(), RdmaCmError> {
    match Mode::from_args() {
        Mode::Server { port } => {
            println!("Creating channel and device id.");
            let listening_id = CommunicationManager::new()?;

            // Bind to local host.
            let address = format!("127.0.0.1:{}", port);
            let address: SocketAddr = address.parse().expect("Unable to parse socket address");

            println!("Server: Binding to port.");
            listening_id.bind(&SockAddr::Inet(InetAddr::from_std(&address)))?;

            println!("Server: Listening for connection...");
            listening_id.listen()?;

            let event = listening_id.get_cm_event()?;

            assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());
            println!("Server: listened return with event!");
            let mut connected_id = event.get_connection_request_id();

            let peer: PeerConnectionData = event.get_private_data().expect("Private data missing!");
            dbg!(peer);
            event.ack();
            println!("Acked ConnectionRequest.");

            let mut pd = connected_id.allocate_protection_domain()?;
            let cq = connected_id.create_cq::<100>()?;
            let mut qp = connected_id.create_qp(&pd, &cq);
            println!("pd, cq, and mr allocated!");

            connected_id.accept()?;
            println!("Server: Accepting client connection.");
            let event = listening_id.get_cm_event()?;
            assert_eq!(RdmaCmEvent::Established, event.get_event());
            event.ack();
            println!("Acked `Established`.");

            let mut memory: RegisteredMemory<u64, 1> = pd.allocate_memory::<u64, 1>();
            memory.as_mut_slice(1)[0] = 42;
            let work = vec![(1, memory)];

            let rdma_write = PostSendOpcode::WrRdmaWrite {
                rkey: peer.rkey,
                remote_address: peer.remote_address,
            };

            qp.post_send(work.iter(), rdma_write);

            let mut done: bool = false;
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

            drop(work);

            let event = connected_id.get_cm_event()?;
            assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
            event.ack();
            connected_id.disconnect()?;

            drop(qp);

            drop(pd);
            drop(connected_id);
            drop(listening_id);
            println!("Done.");
            Ok(())
        }
        Mode::Client { ip_address, port } => {
            println!("Creating channel and device id.");
            let mut cm_connection = CommunicationManager::new()?;

            println!("Client: Reading address info...");
            let addr_info = CommunicationManager::get_address_info(&ip_address, &port.to_string())?;

            unsafe {
                let mut current = addr_info;

                while current != null_mut() {
                    println!("Client: Resolving address...");

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
            println!("Client: AddressResolved event acked.");

            println!("Client: Resolving route...");
            cm_connection.resolve_route(1)?;
            let event = cm_connection.get_cm_event()?;
            assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
            event.ack();
            println!("Client: ResolveRoute event acked.");

            println!("Creating queue pairs.");
            let mut pd = cm_connection.allocate_protection_domain()?;
            let cq = cm_connection.create_cq::<100>()?;
            let qp = cm_connection.create_qp(&pd, &cq);

            let mut memory: RegisteredMemory<u64, 1> = pd.allocate_memory::<u64, 1>();

            let connection_data = PeerConnectionData {
                remote_address: memory.memory.as_mut_ptr(),
                rkey: memory.get_rkey(),
            };

            println!("Sending connection data: {:?}", connection_data);
            cm_connection.connect_with_data(&connection_data)?;
            println!("Client: Connecting...");

            let event = cm_connection.get_cm_event()?;
            assert_eq!(RdmaCmEvent::Established, event.get_event());
            event.ack();

            println!("Waiting for server to write to our memory...");
            unsafe {
                loop {
                    let ptr = memory.memory.as_ptr() as *const [u64; 1];
                    let value: [u64; 1] = std::ptr::read_volatile(ptr);
                    if value != [0] {
                        println!("Server set value to: {:?}", value);
                        break;
                    }
                }
            }

            drop(memory);
            cm_connection.disconnect()?;
            let event = cm_connection.get_cm_event()?;
            assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
            event.ack();

            drop(qp);
            drop(pd);
            drop(cm_connection);
            Ok(())
        }
    }
}
