use std::sync::atomic::AtomicUsize;

use super::buffer::Buffer;

// The caller guarantees that no references currently exist to the value at head
#[inline(always)]
pub unsafe fn buf_write<T, B: Buffer<T>>(buf: &B, head: usize, value: T) {
    let slot = unsafe { &mut *(*buf.at(head)).get() };
    slot.write(value);
}

// The caller guarantees that no references currently exist to the value at tail and the value is
// initialized.
#[inline(always)]
pub unsafe fn buf_read<T, B: Buffer<T>>(buf: &B, tail: usize) -> T {
    let slot = unsafe { &*(*buf.at(tail)).get() };
    slot.assume_init_read()
}

#[derive(Default)]
pub struct AtomicPair {
    pub curr: AtomicUsize,
    pub next: AtomicUsize,
}
