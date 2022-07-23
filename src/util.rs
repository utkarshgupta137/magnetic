use std::mem;
use std::ptr;
use std::sync::atomic::AtomicUsize;

use super::buffer::Buffer;

pub fn alloc<T>(size: usize) -> *mut T {
    let mut vec = Vec::with_capacity(size);
    let ptr = vec.as_mut_ptr();
    mem::forget(vec);
    ptr
}

pub fn dealloc<T>(ptr: *mut T, size: usize) {
    unsafe {
        Vec::from_raw_parts(ptr, 0, size);
    }
}

#[inline(always)]
pub fn buf_write<T, B: Buffer<T>>(buf: &mut B, head: usize, value: T) {
    unsafe { ptr::write(buf.at_mut(head), value) }
}

#[inline(always)]
pub fn buf_read<T, B: Buffer<T>>(buf: &B, tail: usize) -> T {
    unsafe { ptr::read(buf.at(tail)) }
}

#[derive(Default)]
pub struct AtomicPair {
    pub curr: AtomicUsize,
    pub next: AtomicUsize,
}
