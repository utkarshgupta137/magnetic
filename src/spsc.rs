//! Single-producer single-consumer queue
//!
//! The SPSC queue allows for pushing from one thread and popping from another.
//! Each end of the queue can only be owned and accessed from a single thread.
//! In other words, both the `SPSCProducer` and `SPSCConsumer` are `Send` and
//! `!Sync`.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Consumer, Producer};
use super::buffer::Buffer;
use util::{pause, buf_read, buf_write};

//#[repr(C)]
struct SPSCQueue<T, B: Buffer<T>> {
    head: AtomicUsize,
    _pad1: [u8; 56],
    tail: AtomicUsize,
    _pad2: [u8; 56],
    buf: B,
    _marker: PhantomData<T>
}

unsafe impl<T, B: Buffer<T>> Sync for SPSCQueue<T, B> {}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct SPSCConsumer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<SPSCQueue<T, B>>>,
}

unsafe impl<T, B: Buffer<T>> Send for SPSCConsumer<T, B> {}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct SPSCProducer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<SPSCQueue<T, B>>>,
}

unsafe impl<T, B: Buffer<T>> Send for SPSCProducer<T, B> {}

/// Creates a new SPSC queue
///
/// # Examples
///
/// ```
/// use magnetic::spsc::spsc_queue;
/// use magnetic::buffer::dynamic::DynamicBuffer;
/// use magnetic::{Producer, Consumer};
///
/// let (p, c) = spsc_queue(DynamicBuffer::new(32).unwrap());
///
/// p.push(1);
/// assert_eq!(c.pop(), 1);
/// ```
pub fn spsc_queue<T, B: Buffer<T>>(buf: B)
        -> (SPSCProducer<T, B>, SPSCConsumer<T, B>) {
    let queue = SPSCQueue {
        head: AtomicUsize::new(0),
        _pad1: [0; 56],
        tail: AtomicUsize::new(0),
        _pad2: [0; 56],
        buf: buf,
        _marker: PhantomData
    };

    let queue = Arc::new(UnsafeCell::new(queue));

    (
        SPSCProducer { queue: queue.clone() },
        SPSCConsumer { queue: queue },
    )
}

impl<T, B: Buffer<T>> Drop for SPSCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in tail..head {
            buf_read(&self.buf, pos);
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for SPSCProducer<T, B> {
    fn push(&self, value: T) {
        let q = unsafe { &mut *self.queue.get() };
        let head = q.head.load(Ordering::Relaxed);

        while q.tail.load(Ordering::Acquire) + q.buf.size() <= head { pause(); }

        buf_write(&mut q.buf, head, value);
        q.head.store(head + 1, Ordering::Release);
    }

    fn try_push(&self, value: T) -> Option<T> {
        let q = unsafe { &mut *self.queue.get() };
        let head = q.head.load(Ordering::Relaxed);
        if q.tail.load(Ordering::Acquire) + q.buf.size() <= head {
            Some(value)
        } else {
            buf_write(&mut q.buf, head, value);
            q.head.store(head + 1, Ordering::Release);
            None
        }
    }
}

impl<T, B: Buffer<T>> Consumer<T> for SPSCConsumer<T, B> {
    fn pop(&self) -> T {
        let q = unsafe { &mut *self.queue.get() };

        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;
        while tail_plus_one > q.head.load(Ordering::Acquire) { pause(); }

        let v = buf_read(&q.buf, tail);

        q.tail.store(tail_plus_one, Ordering::Release);
        v
    }

    fn try_pop(&self) -> Option<T> {
        let q = unsafe { &mut *self.queue.get() };
        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        if tail_plus_one > q.head.load(Ordering::Acquire) {
            None
        } else {
            let v = buf_read(&q.buf, tail);
            q.tail.store(tail_plus_one, Ordering::Release);
            Some(v)
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread::spawn;

    use test::Bencher;

    use super::*;
    use super::super::{Consumer, Producer};
    use super::super::buffer::dynamic::DynamicBuffer;

    #[test]
    fn one_thread() {
        let (p, c) = spsc_queue(DynamicBuffer::new(2).unwrap());

        p.push(1);
        p.push(2);
        assert_eq!(p.try_push(3), Some(3));
        assert_eq!(c.pop(), 1);
        assert_eq!(p.try_push(4), None);
        assert_eq!(c.pop(), 2);
        assert_eq!(c.try_pop(), Some(4));
        assert_eq!(c.try_pop(), None);
    }

    #[test]
    fn two_thread_seq() {
        let (p, c) = spsc_queue(DynamicBuffer::new(3).unwrap());

        spawn(move || {
            p.push(vec![1; 5]);
            p.push(vec![2; 7]);
            p.push(vec![3; 3]);
        }).join().unwrap();

        spawn(move || {
            assert_eq!(c.pop(), vec![1; 5]);
            assert_eq!(c.pop(), vec![2; 7]);
            assert_eq!(c.pop(), vec![3; 3]);
            assert_eq!(c.try_pop(), None);
        }).join().unwrap();
    }

    #[test]
    fn two_thread_par() {
        let (p, c) = spsc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000;

        let t1 = spawn(move || {
            for i in 0..count {
                p.push(i);
            }
        });

        let t2 = spawn(move || {
            for i in 0..count {
                assert_eq!(c.pop(), i);
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[bench]
    fn ping_pong(b: &mut Bencher) {
        let (p1, c1) = spsc_queue(DynamicBuffer::new(32).unwrap());
        let (p2, c2) = spsc_queue(DynamicBuffer::new(32).unwrap());

        let pong = spawn(move || {
            loop {
                let n = c1.pop();
                p2.push(n);
                if n == 0 {
                    break
                }
            }
        });

        b.iter(|| {
            p1.push(1234);
            c2.pop();
        });

        p1.push(0);
        c2.pop();
        pong.join().unwrap();
    }
}
