pub mod ffi;

use nix::errno::Errno;
use nix::sys::socket::SockAddr;
use std::convert::TryFrom;
use std::ffi::{c_void, CString};
use std::mem::MaybeUninit;

use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::ptr::{null, null_mut};
use std::ops::Deref;

/// Direct translation of rdma cm event types into an enum. Use the bindgen values to ensure our
/// events are correctly labeled even if they change in a different header version.
#[derive(Eq, PartialEq, Debug)]
pub enum RdmaCmEvent {
    AddressResolved,
    AddressError,
    RouteResolved,
    RouteError,
    ConnectionRequest,
    ConnectionResponse,
    ConnectionError,
    Unreachable,
    Rejected,
    Established,
    Disconnected,
    DeviceRemoval,
    MulticastJoin,
    MulticastError,
    AddressChange,
    TimewaitExit,
}

#[allow(non_upper_case_globals)]
impl TryFrom<u32> for RdmaCmEvent {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        use RdmaCmEvent::*;

        let event = match n {
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_ADDR_RESOLVED => AddressResolved,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_ADDR_ERROR => AddressError,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_RESOLVED => RouteResolved,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_ROUTE_ERROR => RouteError,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_REQUEST => ConnectionRequest,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_RESPONSE => ConnectionResponse,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_CONNECT_ERROR => ConnectionError,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_UNREACHABLE => Unreachable,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_REJECTED => Rejected,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_ESTABLISHED => Established,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_DISCONNECTED => Disconnected,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_DEVICE_REMOVAL => DeviceRemoval,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_MULTICAST_JOIN => MulticastJoin,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_MULTICAST_ERROR => MulticastError,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_ADDR_CHANGE => AddressChange,
            ffi::rdma_cm_event_type_RDMA_CM_EVENT_TIMEWAIT_EXIT => TimewaitExit,
            _ => return Err(format!("Unrecognized RDMA cm event value: {}", n)),
        };

        Ok(event)
    }
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

    pub fn get_connection_request_id(&self) -> CommunicatioManager {
        if self.get_event() != RdmaCmEvent::ConnectionRequest {
            panic!("get_connection_request_id only makes sense for ConnectRequest event!");
        }
        let cm_id = unsafe { (*self.event).id };
        CommunicatioManager { cm_id }
    }

    pub fn get_private_data<T: 'static + Copy>(&self) -> Option<T> {
        // TODO: Add other events here?
        match self.get_event() {
            RdmaCmEvent::ConnectionRequest | RdmaCmEvent::Established => {}
            other => {
                panic!("get_private_data not supported for {:?} event.", self.get_event());
            }
        }

        let private_data: *const c_void = unsafe { (*self.event).param.conn.private_data };
        let private_data_length: u8 = unsafe { (*self.event).param.conn.private_data_len };

        // TODO Is there a better way to check the size of the data? This current check is very weak
        // but private_data_length is pretty much useless from the receiver end...
        // We could always tack on the size of the private data as a integer in our private data...
        if std::mem::size_of::<T>() > private_data_length as usize {
            panic!("Size of specified type ({:?}) does not match size of actual data: ({:?}) !", std::mem::size_of::<T>(), private_data_length);
        }

        if private_data.is_null() {
            None
        } else {
            Some(unsafe { *(private_data as *mut T) })
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

impl MemoryRegion {
    pub fn get_rkey(&self) -> u32 {
        unsafe { (*self.mr).rkey }
    }
}

pub struct ProtectionDomain {
    pd: *mut ffi::ibv_pd,
}

pub struct CompletionQueue {
    cq: *mut ffi::ibv_cq,
    // Buffer to hold entries from cq polling.
    // TODO: Make parametric over N (entries).
    // There is a tiny cost overhead associated with this RefCell. Probably not worth optimizing
    // out in exchange for unsafe code?
    // buffer: RefCell<[ffi::ibv_wc; 20]>,
}

impl CompletionQueue {
    // TODO this will panic if we poll() while user still has reference to the returned value.
    // TODO yuck get rid of uncessary box.
    pub fn poll(&self) -> Option<Vec<ffi::ibv_wc>> {
        // zeroed out.
        let mut buffer: [ffi::ibv_wc; 20] = unsafe { std::mem::zeroed() };

        let poll_cq = unsafe {
            (*(*self.cq).context)
                .ops
                .poll_cq
                .expect("Function pointer for post_send missing?")
        };

        let ret = unsafe { poll_cq(self.cq, buffer.len() as i32, buffer.as_mut_ptr()) };
        if ret < 0 {
            panic!("polling cq failed.");
        }
        if ret == 0 {
            return None;
        } else {
            Some(buffer[0..ret as usize].to_vec())
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

/// Uses rdma-cm to manage multiple connections.
pub struct CommunicatioManager {
    cm_id: *mut ffi::rdma_cm_id,
}


impl CommunicatioManager {
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

        CompletionQueue { cq }
    }

    // TODO: Currently we require the use to plz not drop this memory. We should be able to use
    // lifetimes/ownership to assert this at compile time.
    #[must_use = "Please refer to the registered memory via the returned `MemoryRegion`"]
    pub fn register_memory_buffer<T>(&mut self, pd: &ProtectionDomain, memory: &mut Box<[T]>) -> MemoryRegion {
        let mr = unsafe {
            ffi::ibv_reg_mr(
                pd.pd as *const _ as *mut _,
                memory.as_mut_ptr() as *mut _,
                (memory.len() * std::mem::size_of::<T>()) as u64,
                ffi::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as i32,
            )
        };
        if mr == null_mut() {
            panic!("Unable to register_memory");
        }

        MemoryRegion { mr }
    }

    // TODO: Currently we require the use to plz not drop this memory. We should be able to use
    // lifetimes/ownership to assert this at compile time.
    #[must_use = "Please refer to the registered memory via the returned `MemoryRegion`"]
    pub unsafe fn register_memory<T>(pd: &ProtectionDomain, memory: &mut Box<T>) -> MemoryRegion {
        let ptr = memory.as_mut() as *mut T;
        let mr = unsafe {
            ffi::ibv_reg_mr(
                pd.pd as *const _ as *mut _,
                ptr as *mut c_void,
                std::mem::size_of::<T>() as u64,
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
        let ret =
            unsafe { ffi::rdma_create_qp(self.cm_id, pd.pd, &qp_init_attr as *const _ as *mut _) };
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
        CommunicatioManager { cm_id: id }
    }

    pub fn connect<T: 'static>(&self, private_data: Option<&T>) {
        let (ptr, data_size) = CommunicatioManager::check_private_data(private_data);
        dbg!(ptr, data_size);
        let connection_parameters = ffi::rdma_conn_param {
            private_data: ptr,
            private_data_len: data_size,
            responder_resources: 1,
            initiator_depth: 1,
            flow_control: 0,
            retry_count: 0,
            rnr_retry_count: 1,
            srq: 0,
            qp_num: 0,
        };
        let ret =
            unsafe { ffi::rdma_connect(self.cm_id, &connection_parameters as *const _ as *mut _) };
        if ret == -1 {
            panic!("connect failed");
        }
    }

    /// Convert the passed private data into its pointer and length. Returns (null, 0) if None.
    fn check_private_data<T: 'static>(private_data: Option<&T>) -> (*mut c_void, u8){
        let ptr = private_data.map(|data| data as *const _ as *mut T as *mut c_void).unwrap_or(null_mut());
        assert!(std::mem::size_of::<T>() < u8::MAX.into(), "private data too large!");
        let data_size = if ptr.is_null() { 0 } else { std::mem::size_of::<T>() };
        (ptr, data_size as u8)
    }

    pub fn accept<T: 'static>(&self, private_data: Option<&T>) {
        let (ptr, data_size) = CommunicatioManager::check_private_data(private_data);

        // TODO What are the right values for these parameters?
        let connection_parameters = ffi::rdma_conn_param {
            private_data: ptr,
            private_data_len: data_size,
            responder_resources: 1,
            initiator_depth: 1,
            flow_control: 0,
            retry_count: 0,
            rnr_retry_count: 1,
            srq: 0,
            qp_num: 0,
        };
        let ret =
            unsafe { ffi::rdma_accept(self.cm_id, &connection_parameters as *const _ as *mut _) };
        if ret == -1 {
            panic!("accept failed");
        }
    }

    pub fn bind(&self, socket_address: &SockAddr) {
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

    // TODO wrap return value higher level interface. probably iterator!
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

    pub fn resolve_addr(&self, src_addr: Option<SockAddr>, dst_addr: *mut ffi::sockaddr) -> i32 {
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
        let ret = unsafe { ffi::rdma_get_cm_event((*self.cm_id).channel, cm_events.as_mut_ptr()) };
        if ret == -1 {
            panic!("get_cm_event failed!");
        }
        let cm_events = unsafe { cm_events.assume_init() };
        CmEvent { event: cm_events }
    }
}
