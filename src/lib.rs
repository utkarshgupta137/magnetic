//! Magnetic contains a set of high-performance queues useful for developing
//! low-latency applications. All queues are FIFO unless otherwise specified.
//!
//! # Examples
//!
//! ```
//! use std::thread::spawn;
//! use magnetic::spsc::spsc_queue;
//! use magnetic::buffer::dynamic::DynamicBuffer;
//! use magnetic::{Producer, Consumer};
//!
//! let (p, c) = spsc_queue(DynamicBuffer::new(32).unwrap());
//!
//! // Push and pop within a single thread
//! p.push(1);
//! assert_eq!(c.pop(), 1);
//!
//! // Push and pop from multiple threads. Since this example is using the
//! // SPSC queue, only one producer and one consumer are allowed.
//! let t1 = spawn(move || {
//!     for i in 0..10 {
//!         println!("Producing {}", i);
//!         p.push(i);
//!     }
//! });
//!
//! let t2 = spawn(move || {
//!     loop {
//!         let i = c.pop();
//!         println!("Consumed {}", i);
//!         if i == 9 { break; }
//!     }
//! });
//!
//! t1.join().unwrap();
//! t2.join().unwrap();
//! ```

#![feature(asm)]
#![feature(test)]
#![deny(missing_docs)]

extern crate test;

pub mod buffer;
pub mod spsc;
pub mod mpsc;
pub mod spmc;
pub mod mpmc;
mod util;

/// The consumer end of the queue allows for sending data. `Producer<T>` is
/// always `Send`, but is only `Sync` for multi-producer (MPSC, MPMC) queues.
pub trait Producer<T> {
    /// Add value to front of the queue. This method will block if the queue
    /// is currently full.
    fn push(&self, value: T);

    /// Attempt to add a value to the front of the queue. If the value was
    /// added successfully, `None` will be returned. If unsuccessful, `value`
    /// will be returned. An unsuccessful push indicates that the queue was
    /// full.
    fn try_push(&self, value: T) -> Option<T>;
}

/// The consumer end of the queue allows for receiving data. `Consumer<T>` is
/// always `Send`, but is only `Sync` for multi-consumer (SPMC, MPMC) queues.
pub trait Consumer<T> {
    /// Remove value from the end of the queue. This method will block if the
    /// queue is currently empty.
    fn pop(&self) -> T;

    /// Attempt to remove a value from the end of the queue. If the value was
    /// removed successfully, `Some(T)` will be returned. If unsuccessful,
    /// `None` will be returned. An unsuccessful pop indicates that the queue
    /// was empty.
    fn try_pop(&self) -> Option<T>;
}
