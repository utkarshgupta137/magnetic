use std::thread::spawn;

use criterion::{criterion_group, criterion_main, Criterion};

use magnetic::buffer::dynamic::DynamicBuffer;
use magnetic::mpmc::mpmc_queue;
use magnetic::{Consumer, Producer};

fn ping_pong(c: &mut Criterion) {
    let (p1, c1) = mpmc_queue(DynamicBuffer::new(32).unwrap());
    let (p2, c2) = mpmc_queue(DynamicBuffer::new(32).unwrap());

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

    c.bench_function("mpmc::ping_pong", |b| {
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
    let (p1, c1) = mpmc_queue(DynamicBuffer::new(32).unwrap());
    let (p2, c2) = mpmc_queue(DynamicBuffer::new(32).unwrap());

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

    c.bench_function("mpmc::ping_pong_try", |b| {
        b.iter(|| {
            while p1.try_push(1234).is_err() {}
            while c2.try_pop().is_err() {}
        })
    });

    p1.push(0).unwrap();
    c2.pop().unwrap();
    pong.join().unwrap();
}

criterion_group!(benches, ping_pong, ping_pong_try);
criterion_main!(benches);
