use std::{env, time::{SystemTime, UNIX_EPOCH}};
use double_auction::AuctionServer;

fn main() -> std::io::Result<()> {
    let start_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let end_time = start_time + 60;
    let args: Vec<String> = env::args().collect();
    
    if args.len() != 2 {
        eprintln!("Usage: {} <auction_id>", args[0]);
        //eprintln!("Time should be provided in Unix timestamp format (seconds since epoch)");
        std::process::exit(1);
    }
    
    let auction_id = args[1].clone();
    
    let server = AuctionServer::new(auction_id, start_time, end_time);
    server.start()
}