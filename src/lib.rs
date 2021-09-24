#[allow()]
pub mod error;
pub mod ffi;
mod utils;

use nix::sys::socket::SockAddr;
use std::convert::TryFrom;
use std::ffi::{c_void, CString};
use std::mem::MaybeUninit;

use crate::error::RdmaCmError;
pub use crate::error::Result;
use arrayvec::ArrayVec;
use std::cmp::max;
use std::io::Error;
use std::ptr::{null, null_mut};

use std::ops::{Deref, DerefMut};
use std::rc::Rc;

#[allow(unused_imports)]
use tracing::{debug, info, trace};

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
        debug!("{}", function_name!());
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
            panic!(
                "{} only makes sense for ConnectRequest event!",
                fn_basename!()
            );
        }
        let cm_id = unsafe { (*self.event).id };
        CommunicationManager {
            cm_id,
            event_channel: None,
        }
    }

    pub fn get_private_data<T: 'static + Copy>(&self) -> Option<T> {
        // TODO: Add other events here?
        match self.get_event() {
            RdmaCmEvent::ConnectionRequest | RdmaCmEvent::Established => {}
            _other => {
                panic!(
                    "{} not supported for {:?} event.",
                    fn_basename!(),
                    self.get_event()
                );
            }
        }

        let private_data: *const c_void = unsafe { (*self.event).param.conn.private_data };

        if private_data.is_null() {
            None
        } else {
            let private_data: PrivateData<T> = unsafe { *(private_data as *mut PrivateData<T>) };
            assert_eq!(
                std::mem::size_of::<T>(),
                private_data.data_size as usize,
                "Size of private data does not match."
            );
            Some(private_data.data)
        }
    }

    pub fn ack(self) {
        std::mem::drop(self)
    }
}

pub struct RdmaMemory<T, const N: usize> {
    memory: RegisteredMemory<T, N>,
    /// Amounts of bytes actually accessed. Allows us to only send the bytes actually accessed by
    /// user.
    accessed: usize,
    mr: *mut ffi::ibv_mr,
}

enum RegisteredMemory<T, const N: usize> {
    Individual {
        memory: Box<[T; N]>,
    },
    SharedChunk {
        /// We need the starting address to deallocate entire chunk when our ref count hits zero.
        starting_address: *mut [T],
        our_slice: *mut [T],
        /// RC for chunks that share the same underlying registered memory see `register_chunks` fn.
        rc: Rc<()>,
    },
}

/// TODO: We give RdmaMemory directly to the user. If they don't use our IoQueue free function
/// the memory will truly get dropped via this constructor which is slow!
/// Instead, this destructor should do the same thing as IoQueue::free and only when the whole
/// IoQueue is free should we drop the memory.
impl<T, const N: usize> Drop for RdmaMemory<T, N> {
    fn drop(&mut self) {
        debug!("{}", function_name!());

        if let RegisteredMemory::SharedChunk {
            starting_address,
            rc,
            ..
        } = &self.memory
        {
            if Rc::strong_count(&rc) != 1 {
                return;
            }

            // This will take care of dropping the entire chunk for us.
            unsafe { Box::from_raw(*starting_address) };
        }

        let ret = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to ibv_derg_mr memory region: {}", error);
        }
    }
}

impl<T, const N: usize> RdmaMemory<T, N> {
    fn new(mr: *mut ffi::ibv_mr, memory: Box<[T; N]>) -> RdmaMemory<T, N> {
        RdmaMemory {
            memory: RegisteredMemory::Individual { memory },
            mr,
            accessed: 0,
        }
    }

    fn new_chunk(
        mr: *mut ffi::ibv_mr,
        starting_address: *mut [T],
        our_slice: *mut [T],
        rc: Rc<()>,
    ) -> RdmaMemory<T, N> {
        RdmaMemory {
            memory: RegisteredMemory::SharedChunk {
                starting_address,
                our_slice,
                rc,
            },
            mr,
            accessed: 0,
        }
    }

    /// By return a pointer to the underlying array.
    fn as_ptr(&self) -> *const [T; N] {
        match &self.memory {
            RegisteredMemory::Individual { memory } => memory.deref() as *const _,
            RegisteredMemory::SharedChunk { our_slice, .. } => *our_slice as *const _,
        }
    }

    /// By return a mutable pointer to the underlying array.
    fn as_mut_ptr(&mut self) -> *mut [T; N] {
        match &mut self.memory {
            RegisteredMemory::Individual { memory } => memory.deref_mut() as *mut _,
            RegisteredMemory::SharedChunk { our_slice, .. } => *our_slice as *mut [T; N],
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
        assert!(accessed <= N, "Accessing out of range memory.");
        self.accessed = accessed;
    }

    pub fn accessed(&self) -> usize {
        self.accessed
    }

    pub fn as_slice(&self) -> &[T] {
        match &self.memory {
            RegisteredMemory::Individual { memory } => &memory[..self.accessed],
            RegisteredMemory::SharedChunk { our_slice, .. } => unsafe {
                let ptr = *our_slice as *const T;
                std::slice::from_raw_parts(ptr, N)
            },
        }
    }

    /// Return a mutable slice to this memory chunk so it may be written to.
    /// Borrows underlying memory from 0..range.
    pub fn as_mut_slice(&mut self, range: usize) -> &mut [T] {
        assert!(range <= N, "Accessing out of range memory.");

        self.accessed = max(range, self.accessed);
        match &mut self.memory {
            RegisteredMemory::Individual { memory } => &mut memory[..range],
            RegisteredMemory::SharedChunk { our_slice, .. } => unsafe {
                let ptr = *our_slice as *mut T;
                std::slice::from_raw_parts_mut(ptr, N)
            },
        }
    }
}

pub struct ProtectionDomain {
    pd: *mut ffi::ibv_pd,
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        debug!("{}", function_name!());
        let ret = unsafe { ffi::ibv_dealloc_pd(self.pd) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to ibv_dealloc_pd: {}", error);
        }
    }
}
impl ProtectionDomain {
    /// Return a newly allocated array of type T holding N elements.
    pub fn allocate_memory<T: Copy + Default, const N: usize>(&self) -> RdmaMemory<T, N> {
        let memory = utils::vec_to_boxed_array();
        self.do_registration(memory)
    }

    /// Register an existing Boxed array of size [T; N] with the RDMA device returning a
    /// RegisteredMemory of the same size.
    /// By default RegisteredMemory assumes no memory in the array is registered. So
    /// `initialized_elements` may be used to specify how many elements are already initialized.
    pub fn register_array<T, const N: usize>(
        &mut self,
        array: Box<[T; N]>,
        initialized_elements: usize,
    ) -> RdmaMemory<T, N> {
        assert!(array.len() > 0, "No memory allocated for array.");
        assert!(
            initialized_elements < N,
            "Initialized elements bigger than array."
        );
        let mut memory = self.do_registration(array);
        memory.initialize_length(initialized_elements);
        memory
    }

    fn do_registration<T, const N: usize>(&self, mut array: Box<[T; N]>) -> RdmaMemory<T, N> {
        let mr = unsafe {
            ffi::ibv_reg_mr(
                self.pd as *mut _,
                array.as_mut_ptr() as *mut _,
                (array.len() * std::mem::size_of::<T>()) as u64,
                (ffi::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
                    | ffi::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE) as i32,
            )
        };
        if mr == null_mut() {
            panic!("Unable to register_memory");
        }

        RdmaMemory::new(mr, array)
    }

    /// RDMA NICs can handle large contagious registered memory better than many different memory
    /// regions. This function allows you to get `number_of_chunks` different RegisteredMemory
    /// objects that are disjoint regions of the same underlying contagious memory.
    /// TODO: Generalize to <T> instead of u8.     
    pub fn register_chunk<const SIZE: usize>(
        &self,
        num_of_chunks: usize,
    ) -> Vec<RdmaMemory<u8, SIZE>> {
        let mut memory: Box<[u8]> = vec![0 as u8; num_of_chunks * SIZE].into_boxed_slice();

        let mr = unsafe {
            ffi::ibv_reg_mr(
                self.pd as *mut _,
                memory.as_mut_ptr() as *mut _,
                memory.len() as u64,
                ffi::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as i32,
            )
        };
        if mr == null_mut() {
            panic!("Unable to register_memory");
        }

        let raw_memory: *mut [u8] = Box::into_raw(memory);
        let mut chunks: Vec<RdmaMemory<u8, SIZE>> = Vec::with_capacity(num_of_chunks);
        let rc = Rc::new(());

        // Iterate over raw_memory pointer creating new chunks at disjoint memory regions of size
        // SIZE.
        let mut current: *mut [u8; SIZE] = raw_memory as *mut [u8; SIZE];
        for _ in 0..num_of_chunks {
            unsafe {
                chunks.push(RdmaMemory::new_chunk(mr, raw_memory, current, rc.clone()));
                current = current.add(1);
            }
        }

        chunks
    }
}

pub struct CompletionQueue<const POLL_ELEMENTS: usize> {
    cq: *mut ffi::ibv_cq,
}

impl<const POLL_ELEMENTS: usize> Drop for CompletionQueue<POLL_ELEMENTS> {
    fn drop(&mut self) {
        debug!("{}", function_name!());
        let ret = unsafe { ffi::ibv_destroy_cq(self.cq) };
        if ret != 0 {
            let error = Error::last_os_error();
            println!("Unable to ibv_destroy_cq: {}.", error);
        }
    }
}

impl<const POLL_ELEMENTS: usize> CompletionQueue<POLL_ELEMENTS> {
    pub fn poll(&self) -> Option<arrayvec::IntoIter<ffi::ibv_wc, POLL_ELEMENTS>> {
        let mut entries: ArrayVec<ffi::ibv_wc, POLL_ELEMENTS> = ArrayVec::new_const();

        let poll_cq = unsafe {
            (*(*self.cq).context)
                .ops
                .poll_cq
                .expect("Function pointer for poll_cq missing?")
        };

        let ret = unsafe { poll_cq(self.cq, entries.capacity() as i32, entries.as_mut_ptr()) };
        if ret < 0 {
            panic!("polling cq failed.");
        }
        // Initialize the length based on how much poll_cq filled.
        // trace!("Polled entries found: {}", ret);
        if ret == 0 {
            None
        } else {
            unsafe {
                entries.set_len(ret as usize);
            }
            Some(entries.into_iter())
        }
    }
}

/// Wrapper struct for new type pattern so we implement Drop for ffi::ibv_qp.
#[derive(Clone)]
#[allow(non_camel_case_types)]
struct ibv_qp(*mut ffi::ibv_qp);

impl Drop for ibv_qp {
    fn drop(&mut self) {
        debug!("{}", function_name!());
        let ret = unsafe { ffi::ibv_destroy_qp(self.0) };
        if ret != 0 {
            let error = Error::last_os_error();
            eprintln!("Unable to ibv_destroy_qp: {}.", error);
        }
    }
}

/// Allow QueuePair to be Cloned. This is totally safe.
#[derive(Clone)]
pub struct QueuePair {
    qp: Rc<ibv_qp>,
}

impl QueuePair {
    pub fn post_send<'a, I, T, const N: usize>(&mut self, work_requests: I, opcode: PostSendOpcode)
    where
        I: Iterator<Item = &'a (u64, RdmaMemory<T, N>)> + ExactSizeIterator,
        T: 'static + Copy,
    {
        self.post_request::<PostSend, I, T, N, 256>(work_requests, opcode)
    }

    pub fn post_receive<'a, I, T, const N: usize>(&mut self, work_requests: I)
    where
        I: Iterator<Item = &'a (u64, RdmaMemory<T, N>)> + ExactSizeIterator,
        T: 'static + Copy,
    {
        self.post_request::<PostRecv, I, T, N, 256>(work_requests, ())
    }

    fn post_request<'a, R, I, T, const N: usize, const MAX_REQUEST_SIZE: usize>(
        &mut self,
        work_requests: I,
        opcode: <R as Request>::OpCode,
    ) where
        I: Iterator<Item = &'a (u64, RdmaMemory<T, N>)> + ExactSizeIterator,
        // Copy because we only want user sending "dumb" data.
        T: 'static + Copy,
        R: Request,
    {
        assert_ne!(
            work_requests.len(),
            0,
            "memory regions empty! nothing to do."
        );
        assert!(
            work_requests.len() <= MAX_REQUEST_SIZE,
            "Too many requests ({}). Max size reached ({}).",
            work_requests.len(),
            MAX_REQUEST_SIZE,
        );

        let mut requests: ArrayVec<R::WorkRequest, MAX_REQUEST_SIZE> = ArrayVec::new_const();
        let mut sges: ArrayVec<ffi::ibv_sge, MAX_REQUEST_SIZE> = ArrayVec::new_const();

        // Create all entries to fill `sges` and `requests`
        for (i, (work_id, memory)) in work_requests.enumerate() {
            // Total number of bytes to send.
            let length = (R::memory_size(memory) * std::mem::size_of::<T>()) as u32;

            unsafe {
                sges.push_unchecked(ffi::ibv_sge {
                    addr: memory.as_ptr() as u64,
                    length,
                    lkey: memory.get_lkey(),
                });
            }

            let wr = R::make_work_request(*work_id, &mut sges[i] as *mut _, length <= 512, opcode);
            unsafe {
                requests.push_unchecked(wr);
            }
        }

        // Link all entries together.
        R::link(&mut requests);

        let mut bad_wr: MaybeUninit<*mut R::WorkRequest> = MaybeUninit::uninit();
        let ret = R::post(self, requests.as_mut_ptr(), bad_wr.as_mut_ptr());

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
    type OpCode: Copy;

    fn post(
        qp: &mut QueuePair,
        work_requests: *mut Self::WorkRequest,
        bad_work_request: *mut *mut Self::WorkRequest,
    ) -> i32;

    fn memory_size<T, const N: usize>(memory: &RdmaMemory<T, N>) -> usize;

    fn make_work_request(
        wr_id: u64,
        sg_list: *mut ffi::ibv_sge,
        inline: bool,
        opcode: Self::OpCode,
    ) -> Self::WorkRequest;

    fn link<const N: usize>(requests: &mut ArrayVec<Self::WorkRequest, N>);
}

/// TODO Carry around type information <T> for the type?
#[derive(Copy, Clone)]
pub enum PostSendOpcode {
    Send,
    RdmaWrite { rkey: u32, remote_address: *mut u64 },
    RdmaRead { rkey: u32, remote_address: *mut u64 },
}

impl PostSendOpcode {
    fn get_opcode(&self) -> ffi::ibv_wr_opcode {
        match self {
            PostSendOpcode::Send => ffi::ibv_wr_opcode_IBV_WR_SEND,
            PostSendOpcode::RdmaWrite { .. } => ffi::ibv_wr_opcode_IBV_WR_RDMA_WRITE,
            PostSendOpcode::RdmaRead { .. } => ffi::ibv_wr_opcode_IBV_WR_RDMA_READ,
        }
    }
}

enum PostSend {}

impl Request for PostSend {
    type WorkRequest = ffi::ibv_send_wr;
    type OpCode = PostSendOpcode;

    fn post(
        qp: &mut QueuePair,
        work_requests: *mut Self::WorkRequest,
        bad_work_request: *mut *mut Self::WorkRequest,
    ) -> i32 {
        let post_send = unsafe {
            (*(*(*qp).qp.0).context)
                .ops
                .post_send
                .expect("Function pointer for post_send missing?")
        };

        unsafe { post_send(qp.qp.0, work_requests, bad_work_request) }
    }

    /// Only register the memory that has been written to with the device.
    fn memory_size<T, const N: usize>(memory: &RdmaMemory<T, N>) -> usize {
        memory.accessed()
    }

    fn make_work_request(
        wr_id: u64,
        sg_list: *mut ffi::ibv_sge,
        inline: bool,
        opcode: Self::OpCode,
    ) -> Self::WorkRequest {
        // Zero out before filling with our values.
        let wr: Self::WorkRequest = unsafe { std::mem::zeroed() };
        let inline = if inline {
            ffi::ibv_send_flags_IBV_SEND_INLINE
        } else {
            0
        };

        // Values universal to all our operations.
        let mut wr = Self::WorkRequest {
            wr_id,
            sg_list,
            num_sge: 1,
            send_flags: ffi::ibv_send_flags_IBV_SEND_SIGNALED | inline,
            opcode: opcode.get_opcode(),
            ..wr
        };

        match opcode {
            // Nothing more needs to be done for send.
            PostSendOpcode::Send => {}
            PostSendOpcode::RdmaWrite {
                rkey,
                remote_address,
            }
            | PostSendOpcode::RdmaRead {
                rkey,
                remote_address,
            } => {
                let rdma = ffi::ibv_send_wr__bindgen_ty_2 {
                    rdma: ffi::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 {
                        remote_addr: remote_address as u64,
                        rkey,
                    },
                };
                wr.wr = rdma;
            }
        }
        wr
    }

    fn link<const N: usize>(requests: &mut ArrayVec<Self::WorkRequest, N>) {
        for i in 0..requests.len() - 1 {
            requests[i].next = &mut requests[i + 1] as *mut ffi::ibv_send_wr;
        }
    }
}

enum PostRecv {}

impl Request for PostRecv {
    type WorkRequest = ffi::ibv_recv_wr;
    type OpCode = ();

    fn post(
        qp: &mut QueuePair,
        work_requests: *mut Self::WorkRequest,
        bad_work_request: *mut *mut Self::WorkRequest,
    ) -> i32 {
        let post_recv = unsafe {
            (*(*(*qp).qp.0).context)
                .ops
                .post_recv
                .expect("Function pointer for post_recv missing?")
        };

        unsafe { post_recv(qp.qp.0, work_requests, bad_work_request) }
    }

    /// Register the whole memory size with the device.
    fn memory_size<T, const N: usize>(_memory: &RdmaMemory<T, N>) -> usize {
        N
    }

    // Post recvs do not care about inline data.
    fn make_work_request(
        wr_id: u64,
        sg_list: *mut ffi::ibv_sge,
        _inline: bool,
        _config: Self::OpCode,
    ) -> Self::WorkRequest {
        Self::WorkRequest {
            wr_id,
            next: null_mut(),
            sg_list,
            num_sge: 1,
        }
    }

    fn link<const N: usize>(requests: &mut ArrayVec<Self::WorkRequest, N>) {
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
}

impl Drop for CommunicationManager {
    fn drop(&mut self) {
        debug!("{}", function_name!());
        let ret = unsafe { ffi::rdma_destroy_id(self.cm_id) };
        if ret != 0 {
            let error = Error::last_os_error();
            eprintln!("Unable to rdma_destroy_id: {}", error);
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
        info!("{}", function_name!());

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
        info!("{}", function_name!());

        let pd = unsafe { ffi::ibv_alloc_pd(self.get_raw_verbs_context()) };
        if pd == null_mut() {
            return Err(RdmaCmError::ProtectionDomain);
        }
        Ok(ProtectionDomain { pd })
    }

    pub fn create_cq<const ELEMENTS: usize>(&self) -> Result<CompletionQueue<ELEMENTS>> {
        info!("{}", function_name!());

        let cq = unsafe {
            ffi::ibv_create_cq(
                self.get_raw_verbs_context(),
                ELEMENTS as i32,
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

    pub fn create_qp<const ELEMENTS: usize>(
        &self,
        pd: &ProtectionDomain,
        cq: &CompletionQueue<ELEMENTS>,
    ) -> QueuePair {
        info!("{}", function_name!());

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
            qp: unsafe { Rc::new(ibv_qp((*self.cm_id).qp)) },
        }
    }

    pub fn connect(&self) -> Result<()> {
        info!("{}", function_name!());
        self.do_connect::<()>(None)
    }

    /// Use the `private_data` field of `rdma_conn_param` to send data along with the connection.
    pub fn connect_with_data<T: Copy + 'static>(&self, private_data: &T) -> Result<()> {
        info!("{}", function_name!());
        self.do_connect(Some(private_data))
    }

    fn do_connect<T: Copy + 'static>(&self, private_data: Option<&T>) -> Result<()> {
        let mut connection_parameters = ffi::rdma_conn_param {
            private_data: null_mut(),
            private_data_len: 0,
            responder_resources: 1,
            initiator_depth: 1,
            flow_control: 0,
            retry_count: 0,
            rnr_retry_count: 1,
            srq: 0,
            qp_num: 0,
        };

        let ret = match private_data {
            None => unsafe {
                ffi::rdma_connect(self.cm_id, &connection_parameters as *const _ as *mut _)
            },
            Some(private_data) => {
                // Safety: private data must exist in stack for call to rdma_connect. Otherwise
                // .private_data will be a dangling pointer. Do not move rmda_connect outside
                // this match arm.
                let mut private_data = PrivateData::<T>::new(*private_data);
                connection_parameters.private_data = &mut private_data as *mut _ as *mut c_void;
                connection_parameters.private_data_len = private_data.send_size();

                unsafe {
                    ffi::rdma_connect(self.cm_id, &connection_parameters as *const _ as *mut _)
                }
            }
        };

        if ret == -1 {
            return Err(RdmaCmError::Connect(Error::last_os_error()));
        }
        Ok(())
    }

    /// WARNING: The pd, cq, and qp must be allocated before calling `accept` otherwise the program
    /// will get stuck!
    /// TODO: Enforce this at runtime or compile time?
    pub fn accept(&self) -> Result<()> {
        info!("{}", function_name!());
        self.do_accept::<()>(None)
    }

    pub fn accept_with_private_data<T>(&self, private_data: &T) -> Result<()>
    where
        T: Copy + 'static,
    {
        info!("{}", function_name!());
        self.do_accept(Some(private_data))
    }

    fn do_accept<T>(&self, private_data: Option<&T>) -> Result<()>
    where
        T: Copy + 'static,
    {
        let mut connection_parameters = ffi::rdma_conn_param {
            private_data: null_mut(),
            private_data_len: 0,
            responder_resources: 1,
            initiator_depth: 1,
            flow_control: 0,
            retry_count: 0,
            rnr_retry_count: 1,
            srq: 0,
            qp_num: 0,
        };

        let ret = match private_data {
            None => unsafe {
                ffi::rdma_accept(self.cm_id, &connection_parameters as *const _ as *mut _)
            },
            Some(private_data) => {
                let mut private_data = PrivateData::<T>::new(*private_data);
                connection_parameters.private_data = &mut private_data as *mut _ as *mut c_void;
                connection_parameters.private_data_len = private_data.send_size();
                unsafe {
                    ffi::rdma_accept(self.cm_id, &connection_parameters as *const _ as *mut _)
                }
            }
        };

        if ret == -1 {
            return Err(RdmaCmError::Accept(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn bind(&self, socket_address: &SockAddr) -> Result<()> {
        info!("{}. SockAddr: {:?}", function_name!(), socket_address);
        let (addr, _len) = socket_address.as_ffi_pair();

        let ret = unsafe { ffi::rdma_bind_addr(self.cm_id, addr as *const _ as *mut _) };
        if ret == -1 {
            return Err(RdmaCmError::Bind(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn listen(&self) -> Result<()> {
        info!("{}", function_name!());
        let ret = unsafe { ffi::rdma_listen(self.cm_id, 100) };

        if ret == -1 {
            return Err(RdmaCmError::Listen(Error::last_os_error()));
        }
        Ok(())
    }

    // TODO wrap return value higher level interface. probably iterator!
    pub fn get_address_info(node: &str, service: &str) -> Result<*mut ffi::rdma_addrinfo> {
        info!("{}", function_name!());

        let node = CString::new(node).unwrap();
        let service = CString::new(service).unwrap();

        let mut address_info: MaybeUninit<*mut ffi::rdma_addrinfo> = MaybeUninit::uninit();

        let ret = unsafe {
            ffi::rdma_getaddrinfo(
                node.as_ptr(),
                service.as_ptr(),
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
        info!("{}", function_name!());

        assert_ne!(destination_address, null_mut(), "dst_addr is null!");
        let ret = unsafe { ffi::rdma_resolve_addr(self.cm_id, null_mut(), destination_address, 0) };
        if ret == -1 {
            return Err(RdmaCmError::ResolveAddress(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn resolve_route(&self, timeout_ms: i32) -> Result<()> {
        info!("{}", function_name!());

        let ret = unsafe { ffi::rdma_resolve_route(self.cm_id, timeout_ms) };
        if ret == -1 {
            return Err(RdmaCmError::ResolveRoute(Error::last_os_error()));
        }
        Ok(())
    }

    pub fn get_cm_event(&self) -> Result<CmEvent> {
        info!("{}", function_name!());

        let mut cm_events: MaybeUninit<*mut ffi::rdma_cm_event> = MaybeUninit::uninit();
        let ret = unsafe { ffi::rdma_get_cm_event((*self.cm_id).channel, cm_events.as_mut_ptr()) };
        if ret == -1 {
            return Err(RdmaCmError::GetCmEvent(Error::last_os_error()));
        }
        Ok(CmEvent {
            event: unsafe { cm_events.assume_init() },
        })
    }

    pub fn disconnect(&self) -> Result<()> {
        info!("{}", function_name!());

        let ret = unsafe { ffi::rdma_disconnect(self.cm_id) };
        if ret == -1 {
            return Err(RdmaCmError::Disconnect(Error::last_os_error()));
        }

        Ok(())
    }
}

pub struct VolatileRdmaMemory<T, const N: usize> {
    memory: RdmaMemory<T, N>,
}

impl<T, const N: usize> VolatileRdmaMemory<T, N>
where
    T: Copy + Default,
{
    pub fn new(pd: &ProtectionDomain) -> VolatileRdmaMemory<T, N> {
        let memory = pd.allocate_memory();
        VolatileRdmaMemory { memory }
    }

    pub fn as_connection_data(&mut self) -> PeerConnectionData<T, N> {
        PeerConnectionData {
            remote_address: self.memory.as_mut_ptr() as *mut _,
            rkey: self.memory.get_rkey(),
        }
    }

    pub fn read(&self) -> [T; N] {
        unsafe { std::ptr::read_volatile(self.memory.as_ptr()) }
    }

    pub fn write(&mut self, new_value: &[T; N]) {
        unsafe { std::ptr::write_volatile(self.memory.as_mut_ptr(), *new_value) };
    }
}

/// Data necessary for one-sided RDMA to read/write to peer.
#[derive(Clone, Copy, Debug)]
pub struct PeerConnectionData<T, const N: usize> {
    remote_address: *mut T,
    rkey: u32,
}

impl<T, const N: usize> PeerConnectionData<T, N> {
    /// Convinience method for creating the opcode for one-sided RDMA write to peer.
    pub fn as_rdma_write(&self) -> PostSendOpcode {
        PostSendOpcode::RdmaWrite {
            rkey: self.rkey,
            remote_address: self.remote_address as *mut u64,
        }
    }

    pub fn as_rdma_read(&self) -> PostSendOpcode {
        PostSendOpcode::RdmaRead {
            rkey: self.rkey,
            remote_address: self.remote_address as *mut u64,
        }
    }
}

/// Private data to send over RDMA CM.
#[derive(Copy, Clone)]
struct PrivateData<T: 'static + Copy + Clone> {
    data: T,
    /// The size of `T`.
    data_size: u8,
}

impl<T: Copy> PrivateData<T> {
    pub fn new(data: T) -> Self {
        let size = std::mem::size_of::<T>();

        // TODO: Not actually sure what the max size of private data is.
        // Here we compare against Self. Since the entire struct, including our size field
        // needs to be smaller than u8!
        assert!(
            std::mem::size_of::<Self>() < u8::MAX.into(),
            "private data too large!"
        );

        println!(
            "Private data <{}> size: {}",
            std::any::type_name::<T>(),
            size
        );
        PrivateData {
            data,
            data_size: size as u8,
        }
    }

    /// This is NOT the size of T. It is T + sizeof(our other fields).
    pub fn send_size(&self) -> u8 {
        std::mem::size_of::<Self>() as u8
    }
}
