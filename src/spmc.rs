//! Single-producer multiple-consumer queue
//!
//! The SPMC queue allows for pushing from one thread and popping from another.
//! The producer end of the queue may be accessed by a single thread while the
//! consumer end may be accessed by multiple threads. In other words,
//! `SPMCProducer` is `Send` and `!Sync` while `SPMCConsumer` is `Send` and
//! `Sync`.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Consumer, Producer, TryPushError, TryPopError};
use super::buffer::Buffer;
use util::{pause, buf_read, buf_write};

//#[repr(C)]
struct SPMCQueue<T, B: Buffer<T>> {
    head: AtomicUsize,
    _pad1: [u8; 56],
    tail: AtomicUsize,
    next_tail: AtomicUsize,
    _pad2: [u8; 48],
    buf: B,
    _marker: PhantomData<T>
}

unsafe impl<T, B: Buffer<T>> Sync for SPMCQueue<T, B> {}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct SPMCConsumer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<SPMCQueue<T, B>>>,
}

unsafe impl<T, B: Buffer<T>> Send for SPMCConsumer<T, B> {}
unsafe impl<T, B: Buffer<T>> Sync for SPMCConsumer<T, B> {}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct SPMCProducer<T, B: Buffer<T>> {
    queue: Arc<UnsafeCell<SPMCQueue<T, B>>>,
}

unsafe impl<T, B: Buffer<T>> Send for SPMCProducer<T, B> {}
unsafe impl<T, B: Buffer<T>> Sync for SPMCProducer<T, B> {}

/// Creates a new SPMC queue
///
/// # Examples
///
/// ```
/// use magnetic::spmc::spmc_queue;
/// use magnetic::buffer::dynamic::DynamicBuffer;
/// use magnetic::{Producer, Consumer};
///
/// let (p, c) = spmc_queue(DynamicBuffer::new(32).unwrap());
///
/// p.push(1);
/// assert_eq!(c.pop(), 1);
/// ```
pub fn spmc_queue<T, B: Buffer<T>>(buf: B)
        -> (SPMCProducer<T, B>, SPMCConsumer<T, B>) {
    let queue = SPMCQueue {
        head: AtomicUsize::new(0),
        _pad1: [0; 56],
        tail: AtomicUsize::new(0),
        next_tail: AtomicUsize::new(0),
        _pad2: [0; 48],
        buf: buf,
        _marker: PhantomData
    };

    let queue = Arc::new(UnsafeCell::new(queue));

    (
        SPMCProducer { queue: queue.clone() },
        SPMCConsumer { queue: queue },
    )
}

impl<T, B: Buffer<T>> Drop for SPMCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in tail..head {
            buf_read(&self.buf, pos);
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for SPMCProducer<T, B> {
    fn push(&self, value: T) {
        let q = unsafe { &mut *self.queue.get() };
        let head = q.head.load(Ordering::Relaxed);

        while q.tail.load(Ordering::Acquire) + q.buf.size() <= head { pause(); }

        buf_write(&mut q.buf, head, value);
        q.head.store(head + 1, Ordering::Release);
    }

    fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        let q = unsafe { &mut *self.queue.get() };
        let head = q.head.load(Ordering::Relaxed);
        if q.tail.load(Ordering::Acquire) + q.buf.size() <= head {
            Err(TryPushError::Full(value))
        } else {
            buf_write(&mut q.buf, head, value);
            q.head.store(head + 1, Ordering::Release);
            Ok(())
        }
    }
}

impl<T, B: Buffer<T>> Consumer<T> for SPMCConsumer<T, B> {
    fn pop(&self) -> T {
        let q = unsafe { &mut *self.queue.get() };

        let tail = q.next_tail.fetch_add(1, Ordering::Relaxed);
        let tail_plus_one = tail + 1;
        while tail_plus_one > q.head.load(Ordering::Acquire) { pause(); }

        let v = buf_read(&q.buf, tail);

        while q.tail.load(Ordering::Relaxed) < tail { pause(); }
        q.tail.store(tail_plus_one, Ordering::Release);
        v
    }

    fn try_pop(&self) -> Result<T, TryPopError> {
        let q = unsafe { &mut *self.queue.get() };
        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        if tail_plus_one > q.head.load(Ordering::Relaxed) {
            Err(TryPopError::Empty)
        } else {
            if q.next_tail.compare_and_swap(tail, tail_plus_one, Ordering::Acquire) == tail {
                let v = buf_read(&q.buf, tail);
                q.tail.store(tail_plus_one, Ordering::Release);
                Ok(v)
            } else {
                Err(TryPopError::Empty)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread::spawn;

    use test::Bencher;

    use super::*;
    use super::super::{Consumer, Producer, TryPushError, TryPopError};
    use super::super::buffer::dynamic::DynamicBuffer;

    #[test]
    fn one_thread() {
        let (p, c) = spmc_queue(DynamicBuffer::new(2).unwrap());

        p.push(1);
        p.push(2);
        assert_eq!(p.try_push(3), Err(TryPushError::Full(3)));
        assert_eq!(c.pop(), 1);
        assert_eq!(p.try_push(4), Ok(()));
        assert_eq!(c.pop(), 2);
        assert_eq!(c.try_pop(), Ok(4));
        assert_eq!(c.try_pop(), Err(TryPopError::Empty));
    }

    #[test]
    fn two_thread_seq() {
        let (p, c) = spmc_queue(DynamicBuffer::new(3).unwrap());

        spawn(move || {
            p.push(vec![1; 5]);
            p.push(vec![2; 7]);
            p.push(vec![3; 3]);
        }).join().unwrap();

        spawn(move || {
            assert_eq!(c.pop(), vec![1; 5]);
            assert_eq!(c.pop(), vec![2; 7]);
            assert_eq!(c.pop(), vec![3; 3]);
            assert_eq!(c.try_pop(), Err(TryPopError::Empty));
        }).join().unwrap();
    }

    #[test]
    fn four_thread_par() {
        let (p, c) = spmc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000;
        let total = count * 3;

        let t1 = spawn(move || {
            for i in 0..total {
                p.push(i);
            }
        });

        let c2 = Arc::new(c);
        let mut consumers = Vec::new();
        for _ in 0..3 {
            let c = c2.clone();
            consumers.push(spawn(move || {
                (0..count).fold(0u64, |a, _| a + c.pop())
            }));
        }

        t1.join().unwrap();
        let sum = consumers.into_iter()
            .map(|t| t.join().unwrap())
            .fold(0u64, |a, b| a + b);
        assert_eq!(sum, ((total-1) * ((total-1) + 1)) / 2);
    }

    #[bench]
    fn ping_pong(b: &mut Bencher) {
        let (p1, c1) = spmc_queue(DynamicBuffer::new(32).unwrap());
        let (p2, c2) = spmc_queue(DynamicBuffer::new(32).unwrap());

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

    #[bench]
    fn ping_pong_try(b: &mut Bencher) {
        let (p1, c1) = spmc_queue(DynamicBuffer::new(32).unwrap());
        let (p2, c2) = spmc_queue(DynamicBuffer::new(32).unwrap());

        let pong = spawn(move || {
            loop {
                match c1.try_pop() {
                    Ok(n) => {
                        while let Err(_) = p2.try_push(n) {}
                        if n == 0 {
                            break
                        }
                    },
                    Err(_) => {}
                }
            }
        });

        b.iter(|| {
            while let Err(_) = p1.try_push(1234) {};
            while let Err(_) = c2.try_pop() {};
        });

        p1.push(0);
        c2.pop();
        pong.join().unwrap();
    }
}
