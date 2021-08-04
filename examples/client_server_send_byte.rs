use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::error::RdmaCmError;
use rdma_cm::{CommunicationManager, RdmaCmEvent, RegisteredMemory};
use std::net::SocketAddr;
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
    #[structopt(short, long)]
    ip_address: String,
    #[structopt(short, long)]
    port: u16,
}

fn main() -> Result<(), RdmaCmError> {
    let opt = Opt::from_args();
    match opt.mode {
        Mode::Server => {
            println!("Creating channel and device id.");
            let listening_id = CommunicationManager::new()?;

            // Bind to local host.
            let address = format!("{}:{}", opt.ip_address, opt.port);
            let address: SocketAddr = address.parse().expect("Unable to parse socket address");

            println!("Server: Binding to port.");
            listening_id.bind(&SockAddr::Inet(InetAddr::from_std(&address)))?;

            println!("Server: Listening for connection...");
            listening_id.listen()?;

            let event = listening_id.get_cm_event()?;
            assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());
            println!("Server: listened return with event!");
            let connected_id = event.get_connection_request_id();
            event.ack();
            println!("Acked ConnectionRequest.");

            let mut pd = connected_id.allocate_protection_domain()?;
            let cq = connected_id.create_cq(100)?;
            let mut qp = connected_id.create_qp(&pd, &cq);

            println!("Server: Accepting client connection.");
            connected_id.accept::<()>(None)?;

            let event = listening_id.get_cm_event()?;
            assert_eq!(RdmaCmEvent::Established, event.get_event());
            event.ack();
            println!("Acked Established.");

            let v = vec![0].into_boxed_slice();
            let mut memory: RegisteredMemory<[u64]> = pd.register_slice(v);
            let slice = memory.as_mut_slice(1);
            slice[0] = 2;

            println!("pd, cq, and mr allocated!");
            let work = vec![(2, &memory)];

            qp.post_receive(work.into_iter());
            loop {
                match cq.poll() {
                    None => {}
                    Some(entries) => {
                        for e in entries {
                            assert_eq!(2, e.wr_id, "Incorrect work request id.");
                            println!("Value received!");
                            println!("{:?}", memory.as_mut_slice(1));
                            return Ok(());
                        }
                    }
                }
            }
        }
        Mode::Client => {
            println!("Creating channel and device id.");
            let cm_connection = CommunicationManager::new()?;

            let address = format!("{}:{}", opt.ip_address, opt.port);
            let address: SocketAddr = address.parse().expect("Unable to parse socket address");
            println!("Client: Reading address info...");
            let addr_info = CommunicationManager::get_address_info(InetAddr::from_std(&address))?;

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
            cm_connection.resolve_route(0)?;
            let event = cm_connection.get_cm_event()?;
            assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
            event.ack();
            println!("Client: ResolveRoute event acked.");

            println!("Creating queue pairs.");
            let mut pd = cm_connection.allocate_protection_domain()?;
            let cq = cm_connection.create_cq(10)?;
            let mut qp = cm_connection.create_qp(&pd, &cq);

            println!("Client: Connecting.");
            cm_connection.connect::<()>(None)?;

            let event = cm_connection.get_cm_event()?;
            assert_eq!(RdmaCmEvent::Established, event.get_event());

            let v = Vec::from([42]).into_boxed_slice();
            let memory = pd.register_slice(v);
            println!("pd, cq, and mr allocated!");

            let work = vec![(1, &memory)];

            qp.post_send(work.into_iter());
            loop {
                match cq.poll() {
                    None => {}
                    Some(entries) => {
                        for e in entries {
                            assert_eq!(1, e.wr_id, "Incorrect work request id.");
                            println!("Value sent!");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
