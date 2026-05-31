use std::env;
use hedge_replay::{Player, PlayerConfig};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let session = hedge_core::SessionId::new(args[1].parse().unwrap());
    let mut player = Player::open(session, PlayerConfig::default()).unwrap();
    println!("Total records: {}", player.total_records());
    
    let mut prev = None;
    for i in 0..10 {
        if let Some(r) = player.step() {
            println!("Record {}: sequence_no={}, monotonic_ns={}", i, r.sequence_no, r.monotonic_ns);
            if let Some(p) = prev {
                println!("  Delta: {}", r.monotonic_ns - p);
            }
            prev = Some(r.monotonic_ns);
        } else {
            println!("No more records.");
            break;
        }
    }
}
