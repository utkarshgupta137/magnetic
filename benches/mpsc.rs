use std::sync::mpsc::channel;
use std::thread::spawn;

use criterion::{criterion_group, criterion_main, Criterion};

use magnetic::buffer::dynamic::DynamicBuffer;
use magnetic::mpsc::mpsc_queue;
use magnetic::{Consumer, Producer};

fn ping_pong(c: &mut Criterion) {
    let (p1, c1) = mpsc_queue(DynamicBuffer::new(32).unwrap());
    let (p2, c2) = mpsc_queue(DynamicBuffer::new(32).unwrap());

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

    c.bench_function("mpsc::ping_pong", |b| {
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
    let (p1, c1) = mpsc_queue(DynamicBuffer::new(32).unwrap());
    let (p2, c2) = mpsc_queue(DynamicBuffer::new(32).unwrap());

    let pong = spawn(move || {
        loop {
            match c1.try_pop() {
                Ok(n) => {
                    while let Err(_) = p2.try_push(n) {}
                    if n == 0 {
                        break;
                    }
                }
                Err(_) => {}
            }
        }
        (c1, p2)
    });

    c.bench_function("mpsc::ping_pong_try", |b| {
        b.iter(|| {
            while let Err(_) = p1.try_push(1234) {}
            while let Err(_) = c2.try_pop() {}
        })
    });

    p1.push(0).unwrap();
    c2.pop().unwrap();
    pong.join().unwrap();
}

fn ping_pong_std(c: &mut Criterion) {
    let (p1, c1) = channel();
    let (p2, c2) = channel();

    let pong = spawn(move || loop {
        let n = c1.recv().unwrap();
        p2.send(n).unwrap();
        if n == 0 {
            break;
        }
    });

    c.bench_function("mpsc::ping_pong_std", |b| {
        b.iter(|| {
            p1.send(1234).unwrap();
            c2.recv().unwrap();
        })
    });

    p1.send(0).unwrap();
    c2.recv().unwrap();
    pong.join().unwrap();
}

criterion_group!(benches, ping_pong, ping_pong_try, ping_pong_std);
criterion_main!(benches);
