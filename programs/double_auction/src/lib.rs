use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};
use chrono::{DateTime};

// Type alias for the Pubkey to make it more readable
type Pubkey = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyBid {
    pub bidder: Pubkey,
    pub price_per_kwh: u64,
    pub quantity_kwh: u64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyOffer {
    pub seller: Pubkey,
    pub price_per_kwh: u64,
    pub quantity_kwh: u64,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MatchOrders {
    pub bids: Vec<EnergyBid>,
    pub offers: Vec<EnergyOffer>,
}

impl MatchOrders {
    fn new() -> Self {
        MatchOrders {
            bids: Vec::new(),
            offers: Vec::new(),
        }
    }

    fn add_bid(&mut self, bid: EnergyBid) {
        self.bids.push(bid);
        // Sort bids in descending order by price (highest bid first)
        self.bids.sort_by(|a, b| b.price_per_kwh.cmp(&a.price_per_kwh));
    }

    fn add_offer(&mut self, offer: EnergyOffer) {
        self.offers.push(offer);
        // Sort offers in ascending order by price (lowest offer first)
        self.offers.sort_by(|a, b| a.price_per_kwh.cmp(&b.price_per_kwh));
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Message {
    Bid(EnergyBid),
    Offer(EnergyOffer),
    MatchResult,
}

pub struct AuctionServer {
    auction_id: String,
    start_time: i64,
    end_time: i64,
    orders: Arc<Mutex<MatchOrders>>,
}

impl AuctionServer {
    pub fn new(auction_id: String, start_time: i64, end_time: i64) -> Self {
        AuctionServer {
            auction_id,
            start_time,
            end_time,
            orders: Arc::new(Mutex::new(MatchOrders::new())),
        }
    }

    pub fn start(&self) -> std::io::Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        
        if now < self.start_time {
            let sleep_duration = (self.start_time - now) as u64;
            println!("Waiting for auction to start. Sleeping for {} seconds", sleep_duration);
            thread::sleep(Duration::from_secs(sleep_duration));
        }
        
        println!("Auction {} started at {}", self.auction_id, format_timestamp(self.start_time));
        println!("Auction will end at {}", format_timestamp(self.end_time));
        
        // Start the TCP server to accept bids and offers
        let listener = TcpListener::bind("127.0.0.1:7878")?;
        listener.set_nonblocking(true)?;
        
        let orders_clone = Arc::clone(&self.orders);
        
        loop {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            
            // Periodically check if the auction has ended
            if now >= self.end_time {
                println!("Auction {} ended at {}", self.auction_id, format_timestamp(now));
                self.finalize_auction();
                break;
            }
            
            // Attempt to accept connections
            match listener.accept() {
                Ok((stream, addr)) => {
                    println!("New connection: {}", addr);
                    let orders_thread = Arc::clone(&orders_clone);
                    thread::spawn(move || {
                        handle_connection(stream, orders_thread);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No connection available, continue
                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                    break;
                }
            }
            
            // Sleep to avoid busy waiting
            thread::sleep(Duration::from_secs(1));
        }
        
        Ok(())
    }
    
    fn finalize_auction(&self) {
        let orders = self.orders.lock().unwrap();
        
        println!("Auction {} final state:", self.auction_id);
        println!("Bids: {}", orders.bids.len());
        for (i, bid) in orders.bids.iter().enumerate() {
            println!(
                "  Bid #{}: {} kwh at {} per kwh from {}",
                i + 1, bid.quantity_kwh, bid.price_per_kwh, bid.bidder
            );
        }
        
        println!("Offers: {}", orders.offers.len());
        for (i, offer) in orders.offers.iter().enumerate() {
            println!(
                "  Offer #{}: {} kwh at {} per kwh from {}",
                i + 1, offer.quantity_kwh, offer.price_per_kwh, offer.seller
            );
        }
        
        // Match orders
        println!("\nMatching orders:");
        match_orders(&orders);
    }
}

fn handle_connection(mut stream: TcpStream, orders: Arc<Mutex<MatchOrders>>) {
    let mut buffer = [0; 1024];
    
    match stream.read(&mut buffer) {
        Ok(size) => {
            if size == 0 {
                return;
            }
            
            let received_data = String::from_utf8_lossy(&buffer[0..size]);
            
            match serde_json::from_str::<Message>(&received_data) {
                Ok(Message::Bid(bid)) => {
                    println!("Received bid: {:?}", bid);
                    let mut orders: std::sync::MutexGuard<'_, MatchOrders> = orders.lock().unwrap();
                    orders.add_bid(bid);
                    // Send latest bid to DB
                    
                    let response = serde_json::to_string(&Message::MatchResult).unwrap();
                    stream.write_all(response.as_bytes()).unwrap();
                }
                Ok(Message::Offer(offer)) => {
                    println!("Received offer: {:?}", offer);
                    let mut orders: std::sync::MutexGuard<'_, MatchOrders> = orders.lock().unwrap();
                    orders.add_offer(offer);
                    // Send latest offer to DB
                    
                    let response = serde_json::to_string(&Message::MatchResult).unwrap();
                    stream.write_all(response.as_bytes()).unwrap();
                }
                _ => {
                    eprintln!("Invalid message received");
                }
            }
        }
        Err(e) => {
            eprintln!("Error reading from connection: {}", e);
        }
    }
}

fn match_orders(orders: &MatchOrders) {
    let mut bids = orders.bids.clone();
    let mut offers = orders.offers.clone();
    
    let mut matches = Vec::new();
    
    // Match highest bids with lowest offers
    while !bids.is_empty() && !offers.is_empty() {
        let bid = &bids[0];
        let offer = &offers[0];
        
        // Check if bid price is greater than or equal to offer price
        if bid.price_per_kwh >= offer.price_per_kwh {
            // Calculate minimum quantity between bid and offer
            let match_quantity = std::cmp::min(bid.quantity_kwh, offer.quantity_kwh);
            
            println!(
                "Matched: {} kwh at {}-{} per kwh between bidder {} and seller {}",
                match_quantity, offer.price_per_kwh, bid.price_per_kwh, bid.bidder, offer.seller
            );
            
            matches.push((bid.clone(), offer.clone(), match_quantity));
            
            // Update quantities
            let mut bid_clone = bid.clone();
            let mut offer_clone = offer.clone();
            
            bid_clone.quantity_kwh -= match_quantity;
            offer_clone.quantity_kwh -= match_quantity;
            
            // Remove the first bid and offer
            bids.remove(0);
            offers.remove(0);
            
            // If there's remaining quantity, add back to the vector
            if bid_clone.quantity_kwh > 0 {
                bids.insert(0, bid_clone);
            }
            
            if offer_clone.quantity_kwh > 0 {
                offers.insert(0, offer_clone);
            }
        } else {
            // Bid price is less than offer price, no more matches possible
            break;
        }
    }
    
    println!("\nTotal matches: {}", matches.len());
    println!("Unmatched bids: {}", bids.len());
    println!("Unmatched offers: {}", offers.len());
}

fn format_timestamp(timestamp: i64) -> String {
    let dt = DateTime::from_timestamp(timestamp, 0).unwrap();
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

// Command line tool to submit bids and offers for testing
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;
    use std::io::{Read, Write};
    
    #[test]
    fn test_submit_bid() {
        let bid = EnergyBid {
            bidder: "Bidder1".to_string(),
            price_per_kwh: 100,
            quantity_kwh: 50,
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
            seller: "Seller1".to_string(),
            price_per_kwh: 90,
            quantity_kwh: 30,
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
}