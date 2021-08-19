/// Return function name.
/// If this is called from a closure so we omit everything else. So instead of:
/// io_tracker::execution::do_run_process::{{closure}}::{{closure}}:
/// we get:
///io_tracker::execution::do_run_process
#[macro_export]
macro_rules! function_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        match &name[..name.len()].find("::{{closure}}") {
            Some(pos) => &name[..*pos],
            None => &name[..name.len() - 3],
        }
    }};
}

/// Returns only the basename of function and not its full path:
/// io_tracker::execution::do_run_process
/// we get:
/// do_run_process
#[macro_export]
macro_rules! fn_basename {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);

        // Find and cut the rest of the path
        match &name[..name.len() - 3].rfind(':') {
            Some(pos) => &name[pos + 1..name.len() - 3],
            None => &name[..name.len() - 3],
        }
    }};
}

/// Allocates a new array of type T on the heap. This method avoids first allocating a stack array
/// since this may blow up the stack for large arrays.
pub fn vec_to_boxed_array<T: Copy + Default, const N: usize>() -> Box<[T; N]> {
    let boxed_slice: Box<[T]> = vec![Default::default(); N].into_boxed_slice();

    let ptr = Box::into_raw(boxed_slice) as *mut [T; N];

    unsafe { Box::from_raw(ptr) }
}
