use crate::shards::{init_global_queue};
use crate::broker::threadpool::ThreadPool;
use std::net::TcpListener;


pub fn run(addr: &str, shard_count: usize, pool_size: usize) -> std::io::Result<()> {
    init_global_queue(shard_count);
    let listener = TcpListener::bind(addr)?;
    println!("Broker listening on {}", addr);

    let pool = ThreadPool::new(pool_size, shard_count);

    for stream in listener.incoming() {
        if let Ok(s) = stream {
            pool.submit(s);
        }
    }
    Ok(())
}
