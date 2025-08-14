//! Multiple-producer single-consumer queue
//!
//! The MPSC queue allows for pushing from one thread and popping from another.
//! The producer end of the queue may be accessed by multiple threads while
//! the consumer end may only be accessed by a single thread. In other words,
//! the `MPSCProducer` is `Send` and `Sync` while the `MPSCConsumer` is `Send`
//! and `!Sync`.

use std::hint::spin_loop;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;

use super::buffer::Buffer;
use super::{Consumer, PopError, Producer, PushError, TryPopError, TryPushError};
use crate::util::{buf_read, buf_write, AtomicPair};

struct MPSCQueue<T, B: Buffer<T>> {
    head: CachePadded<AtomicPair>,
    tail: CachePadded<AtomicUsize>,
    buf: B,
    consumer: AtomicBool,
    _marker: PhantomData<T>,
}

/// Consumer end of the queue. Implements the trait `Consumer<T>`.
pub struct MPSCConsumer<T, B: Buffer<T>> {
    queue: Arc<MPSCQueue<T, B>>,
    _not_sync: PhantomData<std::cell::Cell<()>>,
}

/// Producer end of the queue. Implements the trait `Producer<T>`.
pub struct MPSCProducer<T, B: Buffer<T>> {
    queue: Arc<MPSCQueue<T, B>>,
}

unsafe impl<T: Send, B: Buffer<T>> Sync for MPSCProducer<T, B> {}

impl<T, B: Buffer<T>> Clone for MPSCProducer<T, B> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

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
/// p.push(1).unwrap();
/// assert_eq!(c.pop(), Ok(1));
/// ```
pub fn mpsc_queue<T, B: Buffer<T>>(buf: B) -> (MPSCProducer<T, B>, MPSCConsumer<T, B>) {
    let queue = MPSCQueue {
        head: CachePadded::new(AtomicPair::default()),
        tail: CachePadded::new(AtomicUsize::new(0)),
        buf,
        consumer: AtomicBool::new(true),
        _marker: PhantomData,
    };

    let queue = Arc::new(queue);

    (
        MPSCProducer {
            queue: queue.clone(),
        },
        MPSCConsumer {
            queue,
            _not_sync: PhantomData,
        },
    )
}

impl<T, B: Buffer<T>> Drop for MPSCQueue<T, B> {
    fn drop(&mut self) {
        let head = self.head.curr.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        for pos in tail..head {
            unsafe { buf_read(&self.buf, pos) };
        }
    }
}

impl<T, B: Buffer<T>> Producer<T> for MPSCProducer<T, B> {
    fn is_closed(&self) -> bool {
        !self.queue.consumer.load(Ordering::Relaxed)
    }

    fn push(&self, value: T) -> Result<(), PushError<T>> {
        let q = &self.queue;
        let head = q.head.next.fetch_add(1, Ordering::Relaxed);

        loop {
            if q.tail.load(Ordering::Acquire) + q.buf.size() > head {
                break;
            } else if !q.consumer.load(Ordering::Acquire) {
                return Err(PushError::Disconnected(value));
            }
            spin_loop();
        }

        unsafe { buf_write(&q.buf, head, value) };
        while q.head.curr.load(Ordering::Relaxed) < head {
            spin_loop();
        }
        q.head.curr.store(head + 1, Ordering::Release);
        Ok(())
    }

    fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        let q = &self.queue;
        loop {
            let head = q.head.curr.load(Ordering::Relaxed);
            let head_plus_one = head + 1;

            if q.tail.load(Ordering::Acquire) + q.buf.size() <= head {
                return if !q.consumer.load(Ordering::Acquire) {
                    Err(TryPushError::Disconnected(value))
                } else {
                    Err(TryPushError::Full(value))
                };
            } else if q
                .head
                .next
                .compare_exchange_weak(head, head_plus_one, Ordering::Acquire, Ordering::Acquire)
                .is_ok()
            {
                unsafe { buf_write(&q.buf, head, value) };
                q.head.curr.store(head_plus_one, Ordering::Release);
                return Ok(());
            }
        }
    }
}

impl<T, B: Buffer<T>> Consumer<T> for MPSCConsumer<T, B> {
    fn is_closed(&self) -> bool {
        Arc::strong_count(&self.queue) < 2
    }

    fn pop(&self) -> Result<T, PopError> {
        let q = &self.queue;
        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        loop {
            if q.head.curr.load(Ordering::Acquire) >= tail_plus_one {
                break;
            } else if Arc::strong_count(q) < 2 {
                return Err(PopError::Disconnected);
            }
            spin_loop();
        }

        let v = unsafe { buf_read(&q.buf, tail) };
        q.tail.store(tail_plus_one, Ordering::Release);
        Ok(v)
    }

    fn try_pop(&self) -> Result<T, TryPopError> {
        let q = &self.queue;
        let tail = q.tail.load(Ordering::Relaxed);
        let tail_plus_one = tail + 1;

        if q.head.curr.load(Ordering::Acquire) < tail_plus_one {
            return if Arc::strong_count(q) < 2 {
                Err(TryPopError::Disconnected)
            } else {
                Err(TryPopError::Empty)
            };
        }

        let v = unsafe { buf_read(&q.buf, tail) };
        q.tail.store(tail_plus_one, Ordering::Release);
        Ok(v)
    }
}

impl<T, B: Buffer<T>> Drop for MPSCConsumer<T, B> {
    fn drop(&mut self) {
        self.queue.consumer.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod test {
    use std::thread::spawn;

    use super::super::buffer::dynamic::DynamicBuffer;
    use super::super::{Consumer, Producer, TryPopError, TryPushError};
    use super::*;

    #[test]
    fn one_thread() {
        let (p, c) = mpsc_queue(DynamicBuffer::new(2).unwrap());

        p.push(1).unwrap();
        p.push(2).unwrap();
        assert_eq!(p.try_push(3), Err(TryPushError::Full(3)));
        assert_eq!(c.pop(), Ok(1));
        assert_eq!(p.try_push(4), Ok(()));
        assert_eq!(c.pop(), Ok(2));
        assert_eq!(c.try_pop(), Ok(4));
        assert_eq!(c.try_pop(), Err(TryPopError::Empty));
    }

    #[test]
    fn two_thread_seq() {
        let (p, c) = mpsc_queue(DynamicBuffer::new(3).unwrap());

        let p = spawn(move || {
            p.push(vec![1; 5]).unwrap();
            p.push(vec![2; 7]).unwrap();
            p.push(vec![3; 3]).unwrap();
            p
        })
        .join()
        .unwrap();

        let c = spawn(move || {
            assert_eq!(c.pop(), Ok(vec![1; 5]));
            assert_eq!(c.pop(), Ok(vec![2; 7]));
            assert_eq!(c.pop(), Ok(vec![3; 3]));
            assert_eq!(c.try_pop(), Err(TryPopError::Empty));
            c
        })
        .join()
        .unwrap();

        drop(p);

        assert_eq!(c.try_pop(), Err(TryPopError::Disconnected));
    }

    #[test]
    fn four_thread_par() {
        if num_cpus::get() < 4 {
            // Test will not finish in time
            eprintln!("skipping four thread test due to cpu resource constraints");
            return;
        }

        let (p, c) = mpsc_queue(DynamicBuffer::new(32).unwrap());

        let count = 10_000_000u64;

        let mut producers = Vec::new();
        for _ in 0..3 {
            let p = p.clone();
            producers.push(spawn(move || {
                for i in 0..count {
                    p.push(i).unwrap();
                }
            }));
        }

        let total = count * producers.len() as u64;
        let t2 = spawn(move || (0..total).fold(0u64, |a, _| a + c.pop().unwrap()));

        for t in producers.into_iter() {
            t.join().unwrap();
        }

        let sum = t2.join().unwrap();
        assert_eq!(sum, (count - 1) * ((count - 1) + 1) * 3 / 2);
    }

    #[test]
    fn disconnect() {
        let (p, c) = mpsc_queue(DynamicBuffer::new(32).unwrap());
        p.push(1).unwrap();
        p.push(2).unwrap();
        std::mem::drop(p);
        assert_eq!(c.pop(), Ok(1));
        assert_eq!(c.pop(), Ok(2));
        assert_eq!(c.pop(), Err(PopError::Disconnected));
        assert_eq!(c.try_pop(), Err(TryPopError::Disconnected));

        let (p, c) = mpsc_queue(DynamicBuffer::new(2).unwrap());
        p.push(1).unwrap();
        std::mem::drop(c);
        assert!(p.is_closed());
        p.push(1).unwrap();
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));

        let (p, c) = mpsc_queue(DynamicBuffer::new(1).unwrap());
        p.push(1).unwrap();
        assert_eq!(p.try_push(2), Err(TryPushError::Full(2)));
        std::mem::drop(c);
        assert_eq!(p.push(2), Err(PushError::Disconnected(2)));
        assert_eq!(p.try_push(2), Err(TryPushError::Disconnected(2)));
    }
}
