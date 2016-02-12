//! Multiple-producer single-consumer queue
//!
//! The MPSC queue allows for pushing from one thread and popping from another.
//! The producer end of the queue may be accessed by multiple threads while
//! the consumer end may only be accessed by a single thread. In other words,
//! the `MPSCProducer` is `Send` and `Sync` while the `MPSCConsumer` is `Send`
//! and `!Sync`.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Consumer, Producer};
use super::buffer::Buffer;
use util::{pause, buf_read, buf_write};

//#[repr(C)]
struct MPSCQueue<T, B: Buffer<T>> {
    head: AtomicUsize,
    next_head: AtomicUsize,
    _pad1: [u8; 48],
    tail: AtomicUsize,
    _pad2: [u8; 56],
    buf: B,
    _marker: PhantomData<T>
}

unsafe impl<T, B: Buffer<T>> Sync for MPSCQueue<T, B> {}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct MPSCConsumer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<MPSCQueue<T, B>>>,
}

unsafe impl<T, B: Buffer<T>> Send for MPSCConsumer<T, B> {}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct MPSCProducer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<MPSCQueue<T, B>>>,
}

unsafe impl<T, B: Buffer<T>> Send for MPSCProducer<T, B> {}
unsafe impl<T, B: Buffer<T>> Sync for MPSCProducer<T, B> {}

/// Creates a new MPSC queue
///
/// # Examples
///
/// ```
/// use magnetic::mpsc::mpsc_queue;
/// use magnetic::buffer::dynamic::DynamicBuffer;
/// use magnetic::{Producer, Consumer};
///
/// let (p, c) = mpsc_queue(DynamicBuffer::new(32).unwrap());
///
/// p.push(1);
/// assert_eq!(c.pop(), 1);
/// ```
pub fn mpsc_queue<T, B: Buffer<T>>(buf: B)
        -> (MPSCProducer<T, B>, MPSCConsumer<T, B>) {
    let queue = MPSCQueue {
        head: AtomicUsize::new(0),
        next_head: AtomicUsize::new(0),
        _pad1: [0; 48],
        tail: AtomicUsize::new(0),
        _pad2: [0; 56],
        buf: buf,
        _marker: PhantomData
    };

    let queue = Arc::new(UnsafeCell::new(queue));

    (
        MPSCProducer { queue: queue.clone() },
        MPSCConsumer { queue: queue },
    )
}

impl<T, B: Buffer<T>> Drop for MPSCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in tail..head {
            buf_read(&self.buf, pos);
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for MPSCProducer<T, B> {
    fn push(&self, value: T) {
        let q = unsafe { &mut *self.queue.get() };

        let head = q.next_head.fetch_add(1, Ordering::Relaxed);
        while q.tail.load(Ordering::Acquire) + q.buf.size() <= head { pause(); }

        buf_write(&mut q.buf, head, value);

        while q.head.load(Ordering::Relaxed) < head { pause(); }
        q.head.store(head + 1, Ordering::Release);
    }

    fn try_push(&self, value: T) -> Option<T> {
        let q = unsafe { &mut *self.queue.get() };

        let head = q.head.load(Ordering::Relaxed);

        if q.tail.load(Ordering::Relaxed) + q.buf.size() <= head {
            Some(value)
        } else {
            let next = head + 1;
            if q.next_head.compare_and_swap(head, next, Ordering::Acquire) == head {
                buf_write(&mut q.buf, head, value);
                q.head.store(next, Ordering::Release);
                None
            } else {
                Some(value)
            }
        }
    }
}

impl<T, B: Buffer<T>> Consumer<T> for MPSCConsumer<T, B> {
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
    use std::sync::Arc;
    use std::thread::spawn;

    use test::Bencher;

    use super::*;
    use super::super::{Consumer, Producer};
    use super::super::buffer::dynamic::DynamicBuffer;

    #[test]
    fn one_thread() {
        let (p, c) = mpsc_queue(DynamicBuffer::new(2).unwrap());

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
        let (p, c) = mpsc_queue(DynamicBuffer::new(3).unwrap());

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
    fn four_thread_par() {
        let (p, c) = mpsc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000u64;

        let p2 = Arc::new(p);

        let mut producers = Vec::new();
        for _ in 0..3 {
            let p = p2.clone();
            producers.push(spawn(move || {
                for i in 0..count {
                    p.push(i);
                }
            }));
        }

        let total = count * producers.len() as u64;
        let t2 = spawn(move || {
            (0..total).fold(0u64, |a, _| a + c.pop())
        });

        for t in producers.into_iter() {
            t.join().unwrap();
        }

        let sum = t2.join().unwrap();
        assert_eq!(sum, (count-1) * ((count-1) + 1) * 3 / 2);
    }

    #[bench]
    fn ping_pong(b: &mut Bencher) {
        let (p1, c1) = mpsc_queue(DynamicBuffer::new(32).unwrap());
        let (p2, c2) = mpsc_queue(DynamicBuffer::new(32).unwrap());

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
