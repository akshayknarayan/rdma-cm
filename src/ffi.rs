#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// We need this due to rust-bindgen producing scary warnings. See:
// https://github.com/rust-lang/rust-bindgen/issues/1651
#![allow(warnings, unused)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));