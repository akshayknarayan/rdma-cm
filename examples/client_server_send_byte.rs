use nix::sys::socket::IpAddr;
use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::{CommunicatioManager, MemoryRegion, RdmaCmEvent, RegisteredMemoryRef};
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;
use std::str::FromStr;
use structopt::StructOpt;

#[derive(Debug)]
enum Mode {
    Client,
    Server,
}

impl FromStr for Mode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Client" | "client" => Ok(Mode::Client),
            "Server" | "server" => Ok(Mode::Server),
            _ => Err("Unknown mode. Available modes: 'client', 'server'."),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "RMDA CM Client/Server",
    about = "Example RDMA CM Client/Server Program."
)]
struct Opt {
    #[structopt(short, long)]
    mode: Mode,
}

fn main() {
    let opt = Opt::from_args();
    match opt.mode {
        Mode::Server => {
            println!("Creating channel and device id.");
            let listening_id = CommunicatioManager::new();

            // Bind to local host.
            let addr = SockAddr::Inet(InetAddr::new(IpAddr::new_v4(127, 0, 0, 1), 4000));

            println!("Server: Binding to port.");
            listening_id.bind(&addr);

            println!("Server: Listening for connection...");
            listening_id.listen();

            let event = listening_id.get_cm_event();
            assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());
            println!("Server: listened return with event!");
            let mut connected_id = event.get_connection_request_id();
            event.ack();
            println!("Acked ConnectionRequest.");

            let pd = connected_id.allocate_pd();
            let mut cq = connected_id.create_cq();
            let mut qp = connected_id.create_qp(&pd, &cq);

            println!("Server: Accepting client connection.");
            connected_id.accept::<()>(None);

            let event = listening_id.get_cm_event();
            assert_eq!(RdmaCmEvent::Established, event.get_event());
            event.ack();
            println!("Acked Established.");

            let mut v = Vec::from([0]).into_boxed_slice();
            let mut mr = CommunicatioManager::register_memory_buffer(&pd, &mut v);
            let memory_reference = RegisteredMemoryRef::new(mr.get_lkey(), v);
            println!("pd, cq, and mr allocated!");
            let work = vec![(2, &memory_reference)];

            qp.post_receive(work.into_iter());
            loop {
                match cq.poll() {
                    None => {}
                    Some(entries) => {
                        for e in entries {
                            assert_eq!(2, e.wr_id, "Incorrect work request id.");
                            println!("Value received!");
                            println!("{:?}", memory_reference.deref());
                            return;
                        }
                    }
                }
            }
        }
        Mode::Client => {
            println!("Creating channel and device id.");
            let mut cm_connection = CommunicatioManager::new();

            println!("Client: Reading address info...");
            let addr_info = CommunicatioManager::get_addr_info();

            unsafe {
                let mut current = addr_info;

                while current != null_mut() {
                    println!("Client: Resolving address...");
                    let ret = cm_connection.resolve_addr(None, (*current).ai_dst_addr);

                    if ret == 0 {
                        println!("Client: Address resolved.");
                        break;
                    }
                    current = (*current).ai_next;
                }
            }

            let event = cm_connection.get_cm_event();
            assert_eq!(RdmaCmEvent::AddressResolved, event.get_event());
            event.ack();
            println!("Client: AddressResolved event acked.");

            println!("Client: Resolving route...");
            cm_connection.resolve_route(0);
            let event = cm_connection.get_cm_event();
            assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
            event.ack();
            println!("Client: ResolveRoute event acked.");

            println!("Creating queue pairs.");
            let pd = cm_connection.allocate_pd();
            let mut cq = cm_connection.create_cq();
            let mut qp = cm_connection.create_qp(&pd, &cq);

            println!("Client: Connecting.");
            cm_connection.connect::<()>(None);

            let event = cm_connection.get_cm_event();
            assert_eq!(RdmaCmEvent::Established, event.get_event());

            let mut v = Vec::from([42]).into_boxed_slice();
            let mut mr = CommunicatioManager::register_memory_buffer(&pd, &mut v);
            let memory_reference = RegisteredMemoryRef::new(mr.get_lkey(), v);
            println!("pd, cq, and mr allocated!");

            let work = vec![(1, &memory_reference)];

            qp.post_send(work.into_iter());
            loop {
                match cq.poll() {
                    None => {}
                    Some(entries) => {
                        for e in entries {
                            assert_eq!(1, e.wr_id, "Incorrect work request id.");
                            println!("Value sent!");
                            return;
                        }
                    }
                }
            }
        }
    }
}
