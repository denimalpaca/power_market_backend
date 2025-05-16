use std::time::{SystemTime, UNIX_EPOCH};
use std::net::TcpStream;
use std::io::{Read, Write};

use crate::{EnergyBid, EnergyOffer, Message};

#[test]
fn test_submit_bid() {
    let bid = EnergyBid {
        bidder: "223d54c9-0e14-4399-9cd1-961faea299dc".to_string(),
        price_per_mwh: 110,
        quantity_mwh: 30,
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
    };
    
    let message = Message::Bid(bid);
    let serialized = serde_json::to_string(&message).unwrap();
    
    let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
    stream.write_all(serialized.as_bytes()).unwrap();
    
    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[0..size]);
    
    println!("Response: {}", response);
}

#[test]
fn test_submit_offer() {
    let offer = EnergyOffer {
        seller: "99a4166f-8451-4a6d-a73f-e5ffa1d8bae2".to_string(),
        price_per_mwh: 80,
        quantity_mwh: 30,
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
    };
    
    let message = Message::Offer(offer);
    let serialized = serde_json::to_string(&message).unwrap();
    
    let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
    stream.write_all(serialized.as_bytes()).unwrap();
    
    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer).unwrap();
    let response = String::from_utf8_lossy(&buffer[0..size]);
    
    println!("Response: {}", response);
}