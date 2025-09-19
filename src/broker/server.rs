use crate::broker::threadpool::ThreadPool;
use crate::shards::init_global_queue;
use std::net::TcpListener;

pub fn run(
    addr: &str,
    shard_count: usize,
    pool_size: usize,
    max_queue_size: usize,
) -> std::io::Result<()> {
    init_global_queue(shard_count);
    let listener = TcpListener::bind(addr)?;
    println!("Broker listening on {}", addr);

    let mut pool = ThreadPool::new(pool_size, shard_count, max_queue_size);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if let Err(e) = pool.submit(s) {
                    eprintln!("Failed to submit connection: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    pool.shutdown();
    Ok(())
}
