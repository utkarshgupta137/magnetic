use std::thread::spawn;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

use magnetic::buffer::dynamic::DynamicBuffer;
use magnetic::spsc::spsc_queue;
use magnetic::{Consumer, Producer};

fn ping_pong(c: &mut Criterion) {
    let (p1, c1) = spsc_queue(DynamicBuffer::new(32).unwrap());
    let (p2, c2) = spsc_queue(DynamicBuffer::new(32).unwrap());

    let pong = spawn(move || {
        loop {
            let n = c1.pop().unwrap();
            p2.push(n).unwrap();
            if n == 0 {
                break;
            }
        }
        (c1, p2)
    });

    c.bench_function("spsc::ping_pong", |b| {
        b.iter(|| {
            p1.push(1234).unwrap();
            c2.pop().unwrap();
        })
    });

    p1.push(0).unwrap();
    c2.pop().unwrap();
    pong.join().unwrap();
}

fn ping_pong_try(c: &mut Criterion) {
    let (p1, c1) = spsc_queue(DynamicBuffer::new(32).unwrap());
    let (p2, c2) = spsc_queue(DynamicBuffer::new(32).unwrap());

    let pong = spawn(move || {
        loop {
            if let Ok(n) = c1.try_pop() {
                while p2.try_push(n).is_err() {}
                if n == 0 {
                    break;
                }
            }
        }
        (c1, p2)
    });

    c.bench_function("spsc::ping_pong_try", |b| {
        b.iter(|| {
            while p1.try_push(1234).is_err() {}
            while c2.try_pop().is_err() {}
        })
    });

    p1.push(0).unwrap();
    c2.pop().unwrap();
    pong.join().unwrap();
}

fn ops(c: &mut Criterion) {
    let (p1, c1) = spsc_queue(DynamicBuffer::new(1024 * 1024 * 1024).unwrap());

    c.bench_function("spsc uncontended push", |b| {
        b.iter_custom(|iters| {
            // Clear the queue
            while c1.try_pop().is_ok() {}

            let start = Instant::now();
            for i in 0..iters {
                p1.try_push(i).unwrap();
            }
            start.elapsed()
        })
    });

    c.bench_function("spsc uncontended push (full)", |b| {
        b.iter_custom(|iters| {
            // Fill the queue
            while p1.try_push(0).is_ok() {}

            let start = Instant::now();
            for i in 0..iters {
                let _ = p1.try_push(i);
            }
            start.elapsed()
        })
    });

    c.bench_function("spsc uncontended pop", |b| {
        b.iter_custom(|iters| {
            // Clear the queue
            while c1.try_pop().is_ok() {}

            // Fill the queue
            for i in 0..iters {
                p1.try_push(i).unwrap();
            }

            let start = Instant::now();
            for _ in 0..iters {
                c1.pop().unwrap();
            }
            start.elapsed()
        })
    });

    c.bench_function("spsc uncontended pop (empty)", |b| {
        b.iter_custom(|iters| {
            // Clear the queue
            while c1.try_pop().is_ok() {}

            let start = Instant::now();
            for _ in 0..iters {
                let _ = c1.try_pop();
            }
            start.elapsed()
        })
    });
}

criterion_group!(benches, ping_pong, ping_pong_try, ops);
criterion_main!(benches);
