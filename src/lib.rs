pub mod ffi;

use nix::errno::Errno;
use nix::sys::socket::SockAddr;
pub use rdma_event::RdmaCmEvent;
use std::convert::TryFrom;
use std::ffi::{c_void, CString};
use std::mem::MaybeUninit;

use std::ptr::{null, null_mut};

// Use module to bring all symbols into scope for the mod. Avoids having "ffi::" in our enum.
mod rdma_event {
    use crate::ffi::*;
    use std::convert::TryFrom;

    /// Direct translation of rdma cm event types into an enum. Use the bindgen values to ensure our
    /// events are correctly labeled even if they change in a different header version.
    #[derive(Eq, PartialEq, Debug)]
    pub enum RdmaCmEvent {
        AddressResolved = rdma_cm_event_type_RDMA_CM_EVENT_ADDR_RESOLVED as isize,
        AddressError = rdma_cm_event_type_RDMA_CM_EVENT_ADDR_ERROR as isize,
        RouteResolved = rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_RESOLVED as isize,
        RouteError = rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_ERROR as isize,
        ConnectionRequest = rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_REQUEST as isize,
        ConnectionResponse = rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_RESPONSE as isize,
        ConnectionError = rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_ERROR as isize,
        Unreachable = rdma_cm_event_type_RDMA_CM_EVENT_UNREACHABLE as isize,
        Rejected = rdma_cm_event_type_RDMA_CM_EVENT_REJECTED as isize,
        Established = rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED as isize,
        Disconnected = rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED as isize,
        DeviceRemoval = rdma_cm_event_type_RDMA_CM_EVENT_DEVICE_REMOVAL as isize,
        MulticastJoin = rdma_cm_event_type_RDMA_CM_EVENT_MULTICAST_JOIN as isize,
        MulticastError = rdma_cm_event_type_RDMA_CM_EVENT_MULTICAST_ERROR as isize,
        AddressChange = rdma_cm_event_type_RDMA_CM_EVENT_ADDR_CHANGE as isize,
        TimewaitExit = rdma_cm_event_type_RDMA_CM_EVENT_TIMEWAIT_EXIT as isize,
    }

    #[allow(non_upper_case_globals)]
    impl TryFrom<u32> for RdmaCmEvent {
        type Error = String;
        fn try_from(n: u32) -> Result<Self, Self::Error> {
            use RdmaCmEvent::*;

            let event = match n {
                rdma_cm_event_type_RDMA_CM_EVENT_ADDR_RESOLVED => AddressResolved,
                rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_RESOLVED => RouteResolved,
                rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_ERROR => RouteError,
                rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_REQUEST => ConnectionRequest,
                rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_RESPONSE => ConnectionResponse,
                rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_ERROR => ConnectionError,
                rdma_cm_event_type_RDMA_CM_EVENT_UNREACHABLE => Unreachable,
                rdma_cm_event_type_RDMA_CM_EVENT_REJECTED => Rejected,
                rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED => Established,
                rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED => Disconnected,
                rdma_cm_event_type_RDMA_CM_EVENT_DEVICE_REMOVAL => DeviceRemoval,
                rdma_cm_event_type_RDMA_CM_EVENT_MULTICAST_JOIN => MulticastJoin,
                _ => return Err(format!("Invalid integer: {}", n)),
            };

            Ok(event)
        }
    }
}

/// Uses rdma-cm to manage multiple connections.
pub struct RdmaRouter {
    cm_id: *mut ffi::rdma_cm_id,
}

/// This is a single event.
pub struct CmEvent {
    event: *mut ffi::rdma_cm_event,
}

impl CmEvent {
    pub fn get_event(&self) -> RdmaCmEvent {
        let e = unsafe { (*self.event).event };
        TryFrom::try_from(e).expect("Unable to convert event integer to enum.")
    }

    pub fn get_connection_request_id(&self) -> RdmaRouter {
        if self.get_event() != RdmaCmEvent::ConnectionRequest {
            panic!("get_connection_request_id only makes sense for ConnectRequest event!");
        }
        let cm_id = unsafe { (*self.event).id };
        RdmaRouter {
            cm_id,
        }
    }

    pub fn ack(self) -> () {
        let ret = unsafe { ffi::rdma_ack_cm_event(self.event) };
        if ret == -1 {
            panic!("Unable to ack event!");
        }
    }
}

pub struct MemoryRegion {
    mr: *mut ffi::ibv_mr,
}

pub struct ProtectionDomain {
    pd: *mut ffi::ibv_pd,
}

pub struct CompletionQueue {
    cq: *mut ffi::ibv_cq,
    // Buffer to hold entries from cq polling.
    // TODO: Make parametric over N (entries).
    buffer: [ffi::ibv_wc; 20],
}

pub struct WorkCompletion {
    wc: ffi::ibv_wc,
}

impl CompletionQueue {
    pub fn poll(&mut self) -> Option<&[ffi::ibv_wc]> {
        let poll_cq = unsafe {
            (*(*self.cq).context)
                .ops
                .poll_cq
                .expect("Function pointer for post_send missing?")
        };

        let ret = unsafe { poll_cq(self.cq, self.buffer.len() as i32, self.buffer.as_mut_ptr()) };
        if ret < 0 {
            panic!("polling cq failed.");
        }
        if ret == 0 {
            return None;
        } else {
            Some(&self.buffer[0..ret as usize])
        }
    }
}

pub struct QueuePair {
    qp: *mut ffi::ibv_qp,
}

impl QueuePair {
    pub fn post_send(&mut self, mr: &mut MemoryRegion, wr_id: u64) {
        let mr = unsafe { *mr.mr };
        let sge = ffi::ibv_sge {
            addr: mr.addr as u64,
            length: mr.length as u32,
            lkey: mr.lkey,
        };

        let work_request: ffi::ibv_send_wr = unsafe { std::mem::zeroed() };
        let work_request = ffi::ibv_send_wr {
            wr_id,
            next: null_mut(),
            sg_list: &sge as *const _ as *mut _,
            num_sge: 1,
            opcode: ffi::ibv_wr_opcode_IBV_WR_SEND,
            send_flags: ffi::ibv_send_flags_IBV_SEND_SIGNALED,
            ..work_request
        };
        let mut bad_wr: MaybeUninit<*mut ffi::ibv_send_wr> = MaybeUninit::uninit();
        let post_send = unsafe {
            (*(*(*self).qp).context)
                .ops
                .post_send
                .expect("Function pointer for post_send missing?")
        };

        let ret = unsafe {
            post_send(
                self.qp,
                &work_request as *const _ as *mut _,
                bad_wr.as_mut_ptr(),
            )
        };
        // Unlike other rdma and ibverbs functions. The return value must be checked against
        // != 0, not == -1.
        if ret != 0 {
            panic!("Failed to post_send.");
        }
    }

    pub fn post_receive(&mut self, mr: &mut MemoryRegion, wr_id: u64) {
        let mr = unsafe { *mr.mr };
        let sge = ffi::ibv_sge {
            addr: mr.addr as u64,
            length: mr.length as u32,
            lkey: mr.lkey,
        };

        let work_request = ffi::ibv_recv_wr {
            wr_id,
            next: null_mut(),
            sg_list: &sge as *const _ as *mut _,
            num_sge: 1,
        };
        let mut bad_wr: MaybeUninit<*mut ffi::ibv_recv_wr> = MaybeUninit::uninit();
        let post_recv = unsafe {
            (*(*(*self).qp).context)
                .ops
                .post_recv
                .expect("Function pointer for post_send missing?")
        };

        let ret = unsafe {
            post_recv(
                self.qp,
                &work_request as *const _ as *mut _,
                bad_wr.as_mut_ptr(),
            )
        };
        // Unlike other rdma and ibverbs functions. The return value must be checked against
        // != 0, not == -1.
        if ret != 0 {
            panic!("Failed to post_send.");
        }
    }
}

impl RdmaRouter {
    fn get_raw_verbs_context(&mut self) -> *mut ffi::ibv_context {
        let context = unsafe { (*self.cm_id).verbs };
        assert_ne!(null_mut(), context, "Context was found to be null!");
        context
    }

    pub fn allocate_pd(&mut self) -> ProtectionDomain {
        let pd = unsafe { ffi::ibv_alloc_pd(self.get_raw_verbs_context()) };
        if pd == null_mut() {
            panic!("allocate_pd failed.");
        }
        ProtectionDomain { pd }
    }

    pub fn create_cq(&mut self) -> CompletionQueue {
        let cq = unsafe {
            ffi::ibv_create_cq(self.get_raw_verbs_context(), 30, null_mut(), null_mut(), 0)
        };
        if cq == null_mut() {
            panic!("Unable to create_qp");
        }

        let buffer: [ffi::ibv_wc; 20] = unsafe { std::mem::zeroed() };
        CompletionQueue { cq, buffer }
    }

    pub fn register_memory(&mut self, pd: &ProtectionDomain, memory: &mut [u8]) -> MemoryRegion {
        let mr = unsafe {
            ffi::ibv_reg_mr(
                pd.pd as *const _ as *mut _,
                memory.as_mut_ptr() as *mut c_void,
                memory.len() as u64,
                ffi::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as i32,
            )
        };
        if mr == null_mut() {
            panic!("Unable to register_memory");
        }

        MemoryRegion { mr }
    }

    pub fn create_qp(&self, pd: &ProtectionDomain, cq: &CompletionQueue) -> QueuePair {
        let qp_init_attr: ffi::ibv_qp_init_attr = ffi::ibv_qp_init_attr {
            qp_context: null_mut(),
            send_cq: cq.cq,
            recv_cq: cq.cq,
            srq: null_mut(),
            cap: ffi::ibv_qp_cap {
                max_send_wr: 30,
                max_recv_wr: 30,
                max_send_sge: 10,
                max_recv_sge: 10,
                max_inline_data: 10,
            },
            qp_type: ffi::ibv_qp_type_IBV_QPT_RC,
            sq_sig_all: 0,
        };
        let ret = unsafe {
            ffi::rdma_create_qp(self.cm_id, pd.pd, &qp_init_attr as *const _ as *mut _)
        };
        if ret == -1 {
            panic!("create_queue_pairs failed!");
        }

        QueuePair {
            qp: unsafe { (*self.cm_id).qp },
        }
    }

    pub fn new() -> Self {
        let event_channel: *mut ffi::rdma_event_channel =
            unsafe { ffi::rdma_create_event_channel() };
        if event_channel == null_mut() {
            panic!("rdma_create_event_channel failed!");
        }

        let mut id: MaybeUninit<*mut ffi::rdma_cm_id> = MaybeUninit::uninit();
        unsafe {
            let ret = ffi::rdma_create_id(
                event_channel,
                id.as_mut_ptr(),
                null_mut(),
                ffi::rdma_port_space_RDMA_PS_TCP,
            );
            if ret == -1 {
                panic!("rdma_create_id failed.")
            }
        }

        let id: *mut ffi::rdma_cm_id = unsafe { id.assume_init() };
        RdmaRouter {
            cm_id: id,
        }
    }

    pub fn connect(&self) {
        // TODO What are the right values for these parameters?
        let connection_parameters = ffi::rdma_conn_param {
            private_data: null(),
            private_data_len: 0,
            responder_resources: 1,
            initiator_depth: 1,
            flow_control: 0,
            retry_count: 0,
            rnr_retry_count: 1,
            srq: 0,
            qp_num: 0,
        };
        let ret = unsafe {
            ffi::rdma_connect(self.cm_id, &connection_parameters as *const _ as *mut _)
        };
        if ret == -1 {
            panic!("connect failed");
        }
    }

    pub fn accept(&self) {
        // TODO What are the right values for these parameters?
        let connection_parameters = ffi::rdma_conn_param {
            private_data: null(),
            private_data_len: 0,
            responder_resources: 1,
            initiator_depth: 1,
            flow_control: 0,
            retry_count: 0,
            rnr_retry_count: 1,
            srq: 0,
            qp_num: 0,
        };
        let ret = unsafe {
            ffi::rdma_accept(self.cm_id, &connection_parameters as *const _ as *mut _)
        };
        if ret == -1 {
            panic!("accept failed");
        }
    }

    pub fn bind(&self, socket_address: SockAddr) {
        let (addr, _len) = socket_address.as_ffi_pair();

        let ret = unsafe { ffi::rdma_bind_addr(self.cm_id, addr as *const _ as *mut _) };
        if ret == -1 {
            let errono = Errno::last();
            panic!("bind failed: {:?}", errono);
        }
    }

    pub fn listen(&self) {
        // TODO: Change file descriptor to NON_BLOCKING.
        let ret = unsafe { ffi::rdma_listen(self.cm_id, 100) };
        if ret == -1 {
            panic!("listen failed.")
        }
    }

    pub fn get_addr_info() -> *mut ffi::rdma_addrinfo {
        // TODO Hardcoded to the address of prometheus10
        let addr = CString::new("198.19.2.10").unwrap();
        let port = CString::new("4000").unwrap();
        let mut address_info: MaybeUninit<*mut ffi::rdma_addrinfo> = MaybeUninit::uninit();

        let ret = unsafe {
            ffi::rdma_getaddrinfo(
                addr.as_ptr(),
                port.as_ptr(),
                null(),
                address_info.as_mut_ptr(),
            )
        };

        if ret == -1 {
            let errono = Errno::last();
            panic!("get_addr_info failed: {:?}", errono);
        }

        unsafe { address_info.assume_init() }
    }

    pub fn resolve_addr(
        &self,
        src_addr: Option<SockAddr>,
        dst_addr: *mut ffi::sockaddr,
    ) -> i32 {
        assert_ne!(dst_addr, null_mut(), "dst_addr is null!");
        unsafe { ffi::rdma_resolve_addr(self.cm_id, null_mut(), dst_addr, 0) }
    }

    pub fn resolve_route(&self, timeout_ms: i32) {
        let ret = unsafe { ffi::rdma_resolve_route(self.cm_id, timeout_ms) };
        if ret == -1 {
            let errono = Errno::last();
            panic!("failed to resolve_route: {:?}", errono);
        }
    }

    pub fn get_cm_event(&self) -> CmEvent {
        let mut cm_events: MaybeUninit<*mut ffi::rdma_cm_event> = MaybeUninit::uninit();
        let ret =
            unsafe { ffi::rdma_get_cm_event((*self.cm_id).channel, cm_events.as_mut_ptr()) };
        if ret == -1 {
            panic!("get_cm_event failed!");
        }
        let cm_events = unsafe { cm_events.assume_init() };
        CmEvent { event: cm_events }
    }
}