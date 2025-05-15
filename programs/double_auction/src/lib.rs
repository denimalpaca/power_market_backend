use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};
use chrono::DateTime;
use dotenv;

mod db;

// Command line tool to submit bids and offers for testing
#[cfg(test)]
mod tests;

// Type alias for the Pubkey to make it more readable
type Pubkey = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyBid {
    pub bidder: Pubkey,
    pub price_per_mwh: u64,
    pub quantity_mwh: u64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyOffer {
    pub seller: Pubkey,
    pub price_per_mwh: u64,
    pub quantity_mwh: u64,
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
        self.bids.sort_by(|a, b| b.price_per_mwh.cmp(&a.price_per_mwh));
    }

    fn add_offer(&mut self, offer: EnergyOffer) {
        self.offers.push(offer);
        // Sort offers in ascending order by price (lowest offer first)
        self.offers.sort_by(|a, b| a.price_per_mwh.cmp(&b.price_per_mwh));
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
        // Load environment variables
        if let Err(e) = dotenv::dotenv() {
            eprintln!("Warning: Failed to load .env file: {}", e);
        }
        
        self.wait_for_start_time()?;
        self.initialize_auction()?;
        self.run_auction_loop()
    }
    
    fn wait_for_start_time(&self) -> std::io::Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
            .as_secs() as i64;
        
        if now < self.start_time {
            let sleep_duration = (self.start_time - now) as u64;
            println!("Waiting for auction to start. Sleeping for {} seconds", sleep_duration);
            thread::sleep(Duration::from_secs(sleep_duration));
        }
        
        Ok(())
    }
    
    fn initialize_auction(&self) -> std::io::Result<()> {
        println!("Auction {} started at {}", self.auction_id, format_timestamp(self.start_time));
        println!("Auction will end at {}", format_timestamp(self.end_time));

        if let Err(e) = db::update_auction_status(&self.auction_id, "active") {
            eprintln!("Failed to update auction status: {}", e);
            // Continue despite DB error
        }
        
        Ok(())
    }
    
    fn run_auction_loop(&self) -> std::io::Result<()> {
        // Start the TCP server to accept bids and offers
        let listener = TcpListener::bind("127.0.0.1:7878")?;
        listener.set_nonblocking(true)?;
        
        let orders_clone = Arc::clone(&self.orders);

        loop {
            if self.check_auction_end()? {
                break;
            }
            
            self.process_connections(&listener, &orders_clone)?;
            
            // Sleep to avoid busy waiting
            thread::sleep(Duration::from_secs(1));
        }
        
        Ok(())
    }
    
    fn check_auction_end(&self) -> std::io::Result<bool> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
            .as_secs() as i64;
            
        if now >= self.end_time {
            println!("Auction {} ended at {}", self.auction_id, format_timestamp(now));
            self.finalize_auction();
            return Ok(true);
        }
        
        Ok(false)
    }
    
    fn process_connections(&self, listener: &TcpListener, orders_clone: &Arc<Mutex<MatchOrders>>) -> std::io::Result<()> {
        // Attempt to accept connections
        match listener.accept() {
            Ok((stream, addr)) => {
                println!("New connection: {}", addr);
                let orders_thread = Arc::clone(orders_clone);
                let auction_id = self.auction_id.clone();
                
                thread::spawn(move || {
                    match handle_connection(stream, Arc::clone(&orders_thread)) {
                        "bid" => {
                            let bid = {
                                let orders = orders_thread.lock().unwrap();
                                orders.bids.last().unwrap().clone()
                            };
                            
                            if let Err(e) = db::add_bid(&auction_id, &bid) {
                                eprintln!("Failed to add bid to database: {}", e);
                            } else {
                                println!("Bid added to database successfully");
                            }
                        }
                        "offer" => {
                            let offer = {
                                let orders = orders_thread.lock().unwrap();
                                orders.offers.last().unwrap().clone()
                            };
                            
                            if let Err(e) = db::add_offer(&auction_id, &offer) {
                                eprintln!("Failed to add offer to database: {}", e);
                            } else {
                                println!("Offer added to database successfully");
                            }
                        }
                        _ => {
                            eprintln!("Unexpected result from connection handler");
                        }
                    };
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No connection available, continue
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
                return Err(e);
            }
        }
        
        Ok(())
    }
    
    fn finalize_auction(&self) {
        let orders = self.orders.lock().unwrap();
        
        println!("Auction {} final state:", self.auction_id);
        println!("Bids: {}", orders.bids.len());
        /*
        for (i, bid) in orders.bids.iter().enumerate() {
            println!(
                "  Bid #{}: {} mwh at {} per mwh from {}",
                i + 1, bid.quantity_mwh, bid.price_per_mwh, bid.bidder
            );
        }
        */
        println!("Offers: {}", orders.offers.len());
        /*
        for (i, offer) in orders.offers.iter().enumerate() {
            println!(
                "  Offer #{}: {} mwh at {} per mwh from {}",
                i + 1, offer.quantity_mwh, offer.price_per_mwh, offer.seller
            );
        }
        */
        // Naive approach, probably want to de-dup bidder and seller IDs
        let participant_count: u64 = (orders.bids.len() + orders.offers.len()).try_into().unwrap();
        
        // Match orders
        println!("\nMatching orders:");
        let matches = match_orders(&orders);

        let clearing_price = matches[0].2;

        let cleared_money_volume: u64 = matches.iter()
        .map(| x | x.0.quantity_mwh)
        .reduce(| acc, e| acc + (clearing_price * e))
        .unwrap()
        + matches.iter()
        .map(| x | x.1.quantity_mwh)
        .reduce(| acc, e| acc + (clearing_price * e))
        .unwrap();

        let cleared_mwh_volume: u64 = matches.iter()
        .map(| x | x.0.quantity_mwh)
        .reduce(| acc, e| acc + e)
        .unwrap()
        + matches.iter()
        .map(| x | x.1.quantity_mwh)
        .reduce(| acc, e| acc + e)
        .unwrap();

        let mut cleared_ids: Vec<&str> = matches.iter()
        .map(| x | x.0.bidder.as_str())
        .collect();
        let mut cleared_seller_ids: Vec<&str> = matches.iter()
        .map(| x | x.1.seller.as_str())
        .collect();
        cleared_ids.append(&mut cleared_seller_ids);
        
        // Update auction status to completed
        if let Err(e) = db::update_auction_status(&self.auction_id, "completed") {
            eprintln!("Failed to update auction status to completed: {}", e);
        }

        // Update all bids and offers that cleared
        if let Err(e) = db::update_order_status(&self.auction_id, cleared_ids, "cleared") {
            eprintln!("Failed to update cleared participants: {}", e);
        }
        
        // Update auction results table
        if let Err(e) = db::add_auction_result(
            &self.auction_id,
            &clearing_price,
            &participant_count,
            &cleared_money_volume,
            &cleared_mwh_volume) {
            eprintln!("Failed to add auction result to table: {}", e);
        }
    }
}

fn handle_connection(mut stream: TcpStream, orders: Arc<Mutex<MatchOrders>>) -> &'static str {
    let mut buffer = [0; 1024];
    
    match stream.read(&mut buffer) {
        Ok(size) => {
            if size == 0 {
                return "";
            }
            
            let received_data = String::from_utf8_lossy(&buffer[0..size]);
            
            match serde_json::from_str::<Message>(&received_data) {
                Ok(Message::Bid(bid)) => {
                    println!("Received bid: {:?}", bid);
                    let mut orders = orders.lock().unwrap();
                    orders.add_bid(bid);
                    let response = serde_json::to_string(&Message::MatchResult).unwrap();
                    stream.write_all(response.as_bytes()).unwrap();
                    return "bid";
                }
                Ok(Message::Offer(offer)) => {
                    println!("Received offer: {:?}", offer);
                    let mut orders = orders.lock().unwrap();
                    orders.add_offer(offer);
                    let response = serde_json::to_string(&Message::MatchResult).unwrap();
                    stream.write_all(response.as_bytes()).unwrap();
                    return "offer";
                }
                _ => {
                    eprintln!("Invalid message received");
                    return "err";
                }
            }
        }
        Err(e) => {
            eprintln!("Error reading from connection: {}", e);
            return "err"
        }
    }
}

fn match_orders(orders: &MatchOrders) -> Vec<(EnergyBid, EnergyOffer, u64)> {
    let mut bids = orders.bids.clone();
    let mut offers = orders.offers.clone();
    
    let mut matches = Vec::new();
    
    // Match highest bids with lowest offers
    while !bids.is_empty() && !offers.is_empty() {
        let bid = &bids[0];
        let offer = &offers[0];
        
        // Check if bid price is greater than or equal to offer price
        if bid.price_per_mwh >= offer.price_per_mwh {
            // Calculate minimum quantity between bid and offer
            let match_quantity = std::cmp::min(bid.quantity_mwh, offer.quantity_mwh);
            
            println!(
                "Matched: {} mwh at {}-{} per mwh between bidder {} and seller {}",
                match_quantity, offer.price_per_mwh, bid.price_per_mwh, bid.bidder, offer.seller
            );
            
            matches.push((bid.clone(), offer.clone(), match_quantity));
            
            // Update quantities
            let mut bid_clone = bid.clone();
            let mut offer_clone = offer.clone();
            
            bid_clone.quantity_mwh -= match_quantity;
            offer_clone.quantity_mwh -= match_quantity;
            
            // Remove the first bid and offer
            bids.remove(0);
            offers.remove(0);
            
            // If there's remaining quantity, add back to the vector
            if bid_clone.quantity_mwh > 0 {
                bids.insert(0, bid_clone);
            }
            
            if offer_clone.quantity_mwh > 0 {
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

    matches
}

fn format_timestamp(timestamp: i64) -> String {
    let dt = DateTime::from_timestamp(timestamp, 0).unwrap();
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}