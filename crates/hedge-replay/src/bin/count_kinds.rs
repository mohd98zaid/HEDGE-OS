use std::env;
use std::collections::HashMap;
use hedge_replay::{Player, PlayerConfig};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let session = hedge_core::SessionId::new(args[1].parse().unwrap());
    let mut player = Player::open(session, PlayerConfig::default()).unwrap();
    let mut counts = HashMap::new();
    while let Some(r) = player.step() {
        let name = format!("{:?}", r.kind);
        *counts.entry(name).or_insert(0) += 1;
    }
    for (k, v) in counts {
        println!("{}: {}", k, v);
    }
}
