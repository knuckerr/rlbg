mod protocol;
mod shards;
mod broker;
#[macro_use]
pub mod logger;


use crate::broker::server;

fn main() -> std::io::Result<()> {
    let addr = "127.0.0.1:4000"; // TCP bind address
    let shard_count = 4;
    let pool_size = 32;
    let queue_size = 100;
    server::run(addr, shard_count, pool_size, queue_size)
}
