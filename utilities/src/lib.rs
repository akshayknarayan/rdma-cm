use rdma_cm::error::RdmaCmError;
use nix;
use nix::unistd::{ForkResult, fork};
use nix::sys::wait::{waitpid, WaitPidFlag};
use nix::sys::signal::{raise, Signal};
use nix::sys::signal::kill;
use nix::sys::socket::{InetAddr, SockAddr, IpAddr};

pub fn run_server_client(server: fn(Box<(dyn Fn() + 'static)>) -> Result<(), RdmaCmError>, client: fn() -> Result<(), RdmaCmError>) -> Result<(), RdmaCmError> {
    unsafe {
        match fork().expect("Cannot fork") {
            ForkResult::Parent { child: pid } => {
                println!("Running parent process.");
                waitpid(pid, Some(WaitPidFlag::WUNTRACED)).expect("Unable to wait for child to be ready");

                // Server will let child continue once it is ready for listening. Avoids races
                // between server and child being ready.
                let server_is_ready = Box::new(move || {
                        kill(pid, Signal::SIGCONT).expect("Cannot SIGCONT Child.");
                });
                println!("Starting Server.");
                server(server_is_ready)?;
            }
            ForkResult::Child => {
                println!("Starting child process");
                println!("SIGSTOPing ourselves...");
                raise(Signal::SIGSTOP).expect("Unable to raise SIGSTOP");

                println!("Running client!");
                client()?;
            },
        }
    }
    Ok(())
}

pub fn ib_device_ip_address() -> Option<IpAddr> {
    let addrs = nix::ifaddrs::getifaddrs().unwrap();
    for ifaddr in addrs {
        if let Some(address) = ifaddr.address {
            // Assume IB interfaces starts with "ibP"
            if ifaddr.interface_name.starts_with("ibP") {
                if let SockAddr::Inet(inet_addr) = address {
                    if let InetAddr::V4(_) = inet_addr {
                        println!("Found IPv4 Address: {}", inet_addr.ip());
                        return Some(inet_addr.ip())
                    }

                }
            }
        }
    }

    None
}