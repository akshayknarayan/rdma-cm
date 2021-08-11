pub mod error;
pub mod ffi;

use nix::sys::socket::{InetAddr, SockAddr};
use std::convert::TryFrom;
use std::ffi::{c_void, CString};
use std::mem::MaybeUninit;

use crate::error::RdmaCmError;
use crate::error::Result;
use std::cmp::max;
use std::io::Error;
use std::os::raw::c_int;
use std::ptr::{null, null_mut};

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
    fn try_from(n: u32) -> std::result::Result<Self, Self::Error> {
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

impl Drop for CmEvent {
    fn drop(&mut self) {
        let ret = unsafe { ffi::rdma_ack_cm_event(self.event) };
        // We do NOT want to panic on a destructor.
        if ret == -1 {
            let error = Error::last_os_error();
            println!("Unable to rdma_ack_cm_event: {}", error);
        }
    }
}

impl CmEvent {
    pub fn get_event(&self) -> RdmaCmEvent {
        let e = unsafe { (*self.event).event };
        // If this ever happens, it is a bug.
        TryFrom::try_from(e).expect("Unable to convert event integer to enum.")
    }

    pub fn get_connection_request_id(&self) -> CommunicationManager {
        if self.get_event() != RdmaCmEvent::ConnectionRequest {
            panic!("get_connection_request_id only makes sense for ConnectRequest event!");
        }
        let cm_id = unsafe { (*self.event).id };
        CommunicationManager {
            cm_id,
            event_channel: None,
            disconnected: false,
        }
    }

    pub fn get_private_data<T: 'static + Copy>(&self) -> Option<T> {
        // TODO: Add other events here?
        match self.get_event() {
            RdmaCmEvent::ConnectionRequest | RdmaCmEvent::Established => {}
            _other => {
                panic!(
                    "get_private_data not supported for {:?} event.",
                    self.get_event()
                );
            }
        }

        let private_data: *const c_void = unsafe { (*self.event).param.conn.private_data };
        let private_data_length: u8 = unsafe { (*self.event).param.conn.private_data_len };

        // TODO Is there a better way to check the size of the data? This current check is very weak
        // but private_data_length is pretty much useless from the receiver end...
        // We could always tack on the size of the private data as a integer in our private data...
        if std::mem::size_of::<T>() > private_data_length as usize {
            panic!(
                "Size of specified type ({:?}) does not match size of actual data: ({:?}) !",
                std::mem::size_of::<T>(),
                private_data_length
            );
        }

        if private_data.is_null() {
            None
        } else {
            Some(unsafe { *(private_data as *mut T) })
        }
    }

    pub fn ack(self) {
        std::mem::drop(self)
    }
}

pub struct RegisteredMemory<T, const N: usize> {
    pub memory: Box<[T; N]>,
    /// Amounts of bytes actually accessed. Allows us to only send the bytes actually accessed by
    /// user.
    accessed: usize,
    mr: *mut ffi::ibv_mr,
}

impl<T, const N: usize> Drop for RegisteredMemory<T, N> {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to ibv_derg_mr memory region: {}", error);
        }
    }
}

impl<T, const N: usize> RegisteredMemory<T, N> {
    fn from_array(mr: *mut ffi::ibv_mr, memory: Box<[T; N]>) -> RegisteredMemory<T, N> {
        RegisteredMemory {
            memory,
            mr,
            accessed: 0,
        }
    }

    fn inner_mr(&self) -> ffi::ibv_mr {
        unsafe { *self.mr }
    }

    pub fn get_rkey(&self) -> u32 {
        self.inner_mr().rkey
    }

    pub fn get_lkey(&self) -> u32 {
        self.inner_mr().lkey
    }

    fn new(mr: *mut ffi::ibv_mr, memory: Box<[T; N]>) -> RegisteredMemory<T, N> {
        RegisteredMemory {
            memory,
            mr,
            accessed: 0,
        }
    }

    /// "clears" out memory. Useful for when this memory is going to be reused.
    pub fn reset_access(&mut self) {
        self.accessed = 0;
    }

    /// Set number of bytes manually since this memory might have been written to by RDMA.
    pub fn initialize_length(&mut self, accessed: usize) {
        assert_eq!(
            self.accessed, 0,
            "This memory seems to have already been initialized!"
        );
        assert!(
            accessed <= self.capacity(),
            "Accessing out of range memory."
        );
        self.accessed = accessed;
    }
    /// Return total size of memory chunk.
    pub fn capacity(&self) -> usize {
        self.memory.len()
    }

    pub fn accessed(&self) -> usize {
        self.accessed
    }

    /// Return a mutable slice to this memory chunk so it may be written to.
    /// Borrows underlying memory from 0..range.
    pub fn as_mut_slice(&mut self, range: usize) -> &mut [T] {
        assert!(range <= self.capacity(), "Accessing out of range memory.");

        self.accessed = max(range, self.accessed);
        &mut self.memory[..range]
    }
}

pub struct ProtectionDomain {
    pd: *mut ffi::ibv_pd,
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_dealloc_pd(self.pd) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to ibv_dealloc_pd: {}", error);
        }
    }
}

impl ProtectionDomain {
    pub fn register_array<T, const N: usize>(
        &mut self,
        mut memory: Box<[T; N]>,
    ) -> RegisteredMemory<T, N> {
        assert!(memory.len() > 0, "No memory allocated for slice.");
        let mr = unsafe {
            ffi::ibv_reg_mr(
                self.pd as *mut _,
                memory.as_mut_ptr() as *mut _,
                (memory.len() * std::mem::size_of::<T>()) as u64,
                ffi::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as i32,
            )
        };
        if mr == null_mut() {
            panic!("Unable to register_memory");
        }

        RegisteredMemory::from_array(mr, memory)
    }

    // pub fn register_slice<T>(&mut self, mut memory: Box<[T]>) -> RegisteredMemory<[T]> {
    //     assert!(memory.len() > 0, "No memory allocated for slice.");
    //     let mr = unsafe {
    //         ffi::ibv_reg_mr(
    //             self.pd as *mut _,
    //             memory.as_mut_ptr() as *mut _,
    //             (memory.len() * std::mem::size_of::<T>()) as u64,
    //             ffi::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as i32,
    //         )
    //     };
    //     if mr == null_mut() {
    //         panic!("Unable to register_memory");
    //     }
    //
    //     RegisteredMemory::from_buffer(mr, memory)
    // }
}

pub struct CompletionQueue {
    cq: *mut ffi::ibv_cq,
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_destroy_cq(self.cq) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to ibv_destroy_cq: {}.", error);
        }
    }
}

impl CompletionQueue {
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

impl Drop for QueuePair {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_destroy_qp(self.qp) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to ibv_destroy_qp: {}.", error);
        }
    }
}

impl QueuePair {
    pub fn post_send<'a, I, T, const N: usize>(&mut self, work_requests: I)
    where
        I: Iterator<Item = (u64, &'a RegisteredMemory<T, N>)> + ExactSizeIterator,
        T: 'static + Copy,
    {
        self.post_request::<PostSend, I, T, N>(work_requests)
    }

    pub fn post_receive<'a, I, T, const N: usize>(&mut self, work_requests: I)
    where
        I: Iterator<Item = (u64, &'a RegisteredMemory<T, N>)> + ExactSizeIterator,
        T: 'static + Copy,
    {
        self.post_request::<PostRecv, I, T, N>(work_requests)
    }

    fn post_request<'a, R, I, T, const N: usize>(&mut self, work_requests: I)
    where
        I: Iterator<Item = (u64, &'a RegisteredMemory<T, N>)> + ExactSizeIterator,
        // Copy because we only want user sending "dumb" data.
        T: 'static + Copy,
        R: Request,
    {
        use std::ops::Deref;

        if work_requests.len() == 0 {
            panic!("memory regions empty! nothing to do.");
        }

        // Maybe stack allocate to avoid allocation cost?
        let mut requests: Vec<R::WorkRequest> = Vec::with_capacity(work_requests.len());
        let mut sges: Vec<ffi::ibv_sge> = Vec::with_capacity(work_requests.len());

        // Create all entries to fill `sges` and `requests`
        for (i, (work_id, memory)) in work_requests.enumerate() {
            // Total number of bytes to send.
            let length = (R::memory_size(memory) * std::mem::size_of::<T>()) as u32;

            sges.push(ffi::ibv_sge {
                addr: memory.memory.deref() as *const [T] as *const u8 as u64,
                length,
                lkey: memory.get_lkey(),
            });

            let wr = R::make_work_request(work_id, &mut sges[i] as *mut _, length <= 512);
            requests.push(wr);
        }

        // Link all entries together.
        R::link(&mut requests);

        let mut bad_wr: MaybeUninit<*mut R::WorkRequest> = MaybeUninit::uninit();
        let request = R::post(self);
        let ret = unsafe { request(self.qp, &mut requests[0] as *mut _, bad_wr.as_mut_ptr()) };
        // Unlike other rdma and ibverbs functions. The return value must be checked against
        // != 0, not == -1.
        if ret != 0 {
            let error = Error::last_os_error();
            panic!("Failed to post request: {}", error);
        }
    }
}

trait Request {
    type WorkRequest;
    fn post(
        qp: &mut QueuePair,
    ) -> unsafe extern "C" fn(
        *mut ffi::ibv_qp,
        *mut Self::WorkRequest,
        *mut *mut Self::WorkRequest,
    ) -> i32;

    fn memory_size<T, const N: usize>(memory: &RegisteredMemory<T, N>) -> usize;

    fn make_work_request(wr_id: u64, sg_list: *mut ffi::ibv_sge, inline: bool)
        -> Self::WorkRequest;

    fn link(requests: &mut Vec<Self::WorkRequest>);
}

enum PostSend {}

impl Request for PostSend {
    type WorkRequest = ffi::ibv_send_wr;

    fn post(
        qp: &mut QueuePair,
    ) -> unsafe extern "C" fn(
        *mut ffi::ibv_qp,
        *mut Self::WorkRequest,
        *mut *mut Self::WorkRequest,
    ) -> i32 {
        unsafe {
            (*(*(*qp).qp).context)
                .ops
                .post_send
                .expect("Function pointer for post_send missing?")
        }
    }

    /// Only register the memory that has been written to with the device.
    fn memory_size<T, const N: usize>(memory: &RegisteredMemory<T, N>) -> usize {
        memory.accessed()
    }

    fn make_work_request(
        wr_id: u64,
        sg_list: *mut ffi::ibv_sge,
        inline: bool,
    ) -> Self::WorkRequest {
        let work_request_template: Self::WorkRequest = unsafe { std::mem::zeroed() };
        let inline = if inline {
            ffi::ibv_send_flags_IBV_SEND_INLINE
        } else {
            0
        };

        Self::WorkRequest {
            wr_id,
            sg_list,
            num_sge: 1,
            opcode: ffi::ibv_wr_opcode_IBV_WR_SEND,
            send_flags: ffi::ibv_send_flags_IBV_SEND_SIGNALED | inline,
            ..work_request_template
        }
    }

    fn link(requests: &mut Vec<Self::WorkRequest>) {
        for i in 0..requests.len() - 1 {
            requests[i].next = &mut requests[i + 1] as *mut ffi::ibv_send_wr;
        }
    }
}

enum PostRecv {}

impl Request for PostRecv {
    type WorkRequest = ffi::ibv_recv_wr;

    fn post(
        qp: &mut QueuePair,
    ) -> unsafe extern "C" fn(
        *mut ffi::ibv_qp,
        *mut Self::WorkRequest,
        *mut *mut Self::WorkRequest,
    ) -> i32 {
        unsafe {
            (*(*(*qp).qp).context)
                .ops
                .post_recv
                .expect("Function pointer for post_recv missing?")
        }
    }

    /// Register the whole memory size with the device.
    fn memory_size<T, const N: usize>(memory: &RegisteredMemory<T, N>) -> usize {
        memory.capacity()
    }

    // Post recvs do not care about inline data.
    fn make_work_request(
        wr_id: u64,
        sg_list: *mut ffi::ibv_sge,
        _inline: bool,
    ) -> Self::WorkRequest {
        Self::WorkRequest {
            wr_id,
            next: null_mut(),
            sg_list,
            num_sge: 1,
        }
    }

    fn link(requests: &mut Vec<Self::WorkRequest>) {
        for i in 0..requests.len() - 1 {
            requests[i].next = &mut requests[i + 1] as *mut Self::WorkRequest;
        }
    }
}

/// Uses rdma-cm to manage multiple connections.
pub struct CommunicationManager {
    cm_id: *mut ffi::rdma_cm_id,
    /// If the CommunicationManager is used for connecting two nodes it needs an event channel.
    /// We keep a reference to it for deallocation.
    event_channel: Option<*mut ffi::rdma_event_channel>,
    /// Whether user explicitly called disconnect or not?
    disconnected: bool,
}

impl Drop for CommunicationManager {
    fn drop(&mut self) {
        let ret = unsafe { ffi::rdma_destroy_id(self.cm_id) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to rdma_destroy_id: {}", error);
        }

        // Not all ids have an event channel. Only the listening id for the initial pairing between
        // queue pairs do.
        // This call should return a status int but it doesn't... Nothing we can do...
        if let Some(event_channel) = self.event_channel {
            unsafe { ffi::rdma_destroy_event_channel(event_channel) };
        }
    }
}

impl CommunicationManager {
    pub fn new() -> Result<Self> {
        let event_channel: *mut ffi::rdma_event_channel =
            unsafe { ffi::rdma_create_event_channel() };
        if event_channel == null_mut() {
            return Err(RdmaCmError::RdmaEventChannel(Error::last_os_error()));
        }

        let mut id: MaybeUninit<*mut ffi::rdma_cm_id> = MaybeUninit::uninit();
        let ret = unsafe {
            ffi::rdma_create_id(
                event_channel,
                id.as_mut_ptr(),
                null_mut(),
                ffi::rdma_port_space_RDMA_PS_TCP,
            )
        };
        if ret == -1 {
            return Err(RdmaCmError::RdmaCreateId(Error::last_os_error()));
        }

        Ok(CommunicationManager {
            cm_id: unsafe { id.assume_init() },
            event_channel: Some(event_channel),
            disconnected: false,
        })
    }

    /// Convenience method for accessing context and checkingn nullness. Used by other methods.
    fn get_raw_verbs_context(&self) -> *mut ffi::ibv_context {
        // Safety: always safe. Our API guarantees dereferencing `cm_id` will always be valid.
        let context = unsafe { (*self.cm_id).verbs };
        // This would represent a bug in our implementation.
        assert_ne!(
            null_mut(),
            context,
            "Missing ibverbs context. It was found to be null!"
        );
        context
    }

    pub fn allocate_protection_domain(&self) -> Result<ProtectionDomain> {
        let pd = unsafe { ffi::ibv_alloc_pd(self.get_raw_verbs_context()) };
        if pd == null_mut() {
            return Err(RdmaCmError::ProtectionDomain);
        }
        Ok(ProtectionDomain { pd })
    }

    pub fn create_cq(&self, entries: c_int) -> Result<CompletionQueue> {
        let cq = unsafe {
            ffi::ibv_create_cq(
                self.get_raw_verbs_context(),
                entries,
                null_mut(),
                null_mut(),
                0,
            )
        };
        if cq == null_mut() {
            return Err(RdmaCmError::CreateCompletionQueue(Error::last_os_error()));
        }

        Ok(CompletionQueue { cq })
    }

    pub fn create_qp(&self, pd: &ProtectionDomain, cq: &CompletionQueue) -> QueuePair {
        let qp_init_attr: ffi::ibv_qp_init_attr = ffi::ibv_qp_init_attr {
            qp_context: null_mut(),
            send_cq: cq.cq,
            recv_cq: cq.cq,
            srq: null_mut(),
            cap: ffi::ibv_qp_cap {
                max_send_wr: 256,
                max_recv_wr: 256,
                max_send_sge: 10,
                max_recv_sge: 10,
                max_inline_data: 512,
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

    pub fn connect<T: 'static>(&self, private_data: Option<&T>) -> Result<()> {
        let (ptr, data_size) = CommunicationManager::check_private_data(private_data);
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
            return Err(RdmaCmError::Connect(Error::last_os_error()));
        }
        Ok(())
    }

    /// Convert the passed private data into its pointer and length. Returns (null, 0) if None.
    fn check_private_data<T: 'static>(private_data: Option<&T>) -> (*mut c_void, u8) {
        let ptr = private_data
            .map(|data| data as *const _ as *mut T as *mut c_void)
            .unwrap_or(null_mut());
        assert!(
            std::mem::size_of::<T>() < u8::MAX.into(),
            "private data too large!"
        );
        let data_size = if ptr.is_null() {
            0
        } else {
            std::mem::size_of::<T>()
        };
        (ptr, data_size as u8)
    }

    pub fn accept<T: 'static>(&self, private_data: Option<&T>) -> Result<()> {
        let (ptr, data_size) = CommunicationManager::check_private_data(private_data);

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
            return Err(RdmaCmError::Accept(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn bind(&self, socket_address: &SockAddr) -> Result<()> {
        let (addr, _len) = socket_address.as_ffi_pair();

        let ret = unsafe { ffi::rdma_bind_addr(self.cm_id, addr as *const _ as *mut _) };
        if ret == -1 {
            return Err(RdmaCmError::Bind(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn listen(&self) -> Result<()> {
        let ret = unsafe { ffi::rdma_listen(self.cm_id, 100) };

        if ret == -1 {
            return Err(RdmaCmError::Listen(Error::last_os_error()));
        }
        Ok(())
    }

    // TODO wrap return value higher level interface. probably iterator!
    pub fn get_address_info(inet_address: InetAddr) -> Result<*mut ffi::rdma_addrinfo> {
        let addr = CString::new(format!("{}", inet_address.ip())).unwrap();
        let port = CString::new(format!("{}", inet_address.port())).unwrap();

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
            return Err(RdmaCmError::GetAddressInfo(Error::last_os_error()));
        }

        Ok(unsafe { address_info.assume_init() })
    }

    pub fn resolve_address(&self, destination_address: *mut ffi::sockaddr) -> Result<()> {
        assert_ne!(destination_address, null_mut(), "dst_addr is null!");
        let ret = unsafe { ffi::rdma_resolve_addr(self.cm_id, null_mut(), destination_address, 0) };
        if ret == -1 {
            return Err(RdmaCmError::ResolveAddress(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn resolve_route(&self, timeout_ms: i32) -> Result<()> {
        let ret = unsafe { ffi::rdma_resolve_route(self.cm_id, timeout_ms) };
        if ret == -1 {
            return Err(RdmaCmError::ResolveRoute(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn get_cm_event(&self) -> Result<CmEvent> {
        let mut cm_events: MaybeUninit<*mut ffi::rdma_cm_event> = MaybeUninit::uninit();
        let ret = unsafe { ffi::rdma_get_cm_event((*self.cm_id).channel, cm_events.as_mut_ptr()) };
        if ret == -1 {
            return Err(RdmaCmError::GetCmEvent(Error::last_os_error()));
        }
        Ok(CmEvent {
            event: unsafe { cm_events.assume_init() },
        })
    }

    pub fn disconnect(&mut self) -> Result<()> {
        let ret = unsafe { ffi::rdma_disconnect(self.cm_id) };
        if ret == -1 {
            return Err(RdmaCmError::Disconnect(Error::last_os_error()));
        }

        self.disconnected = true;
        Ok(())
    }
}
