use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use postgrest::Postgrest;
use serde::{Serialize, Deserialize};
use chrono::{DateTime};
use dotenv;

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
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        
        if now < self.start_time {
            let sleep_duration = (self.start_time - now) as u64;
            println!("Waiting for auction to start. Sleeping for {} seconds", sleep_duration);
            thread::sleep(Duration::from_secs(sleep_duration));
        }
        
        println!("Auction {} started at {}", self.auction_id, format_timestamp(self.start_time));
        println!("Auction will end at {}", format_timestamp(self.end_time));

        dotenv::dotenv().ok();

        match update_db_auction_active( self.auction_id.clone()) {
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()) => { println!("Supabase updated successfully"); }
            Err(e) => { eprintln!("Supabase update failed with error: {}", e)}
        };
        
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
                    let orders_post_thread = Arc::clone(&orders_clone);
                    let auction_id = self.auction_id.clone();
                    thread::spawn(move || {
                        match handle_connection(stream, orders_thread) {
                            "bid" => {
                                let new_bid = orders_post_thread.lock().unwrap().bids.last().unwrap().clone();
                                match add_bid_to_db(auction_id, new_bid) {
                                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()) => { println!("Supabase updated successfully"); }
                                    Err(e) => { eprintln!("Supabase update failed with error: {}", e)}
                                };
                            }
                            "offer" => {
                                let new_offer = orders_post_thread.lock().unwrap().offers.last().unwrap().clone();
                                match add_offer_to_db(auction_id, new_offer) {
                                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()) => { println!("Supabase updated successfully"); }
                                    Err(e) => { eprintln!("Supabase update failed with error: {}", e)}
                                };
                            }
                            _ => { eprintln!("Unexpected match result, no Supabase update performed on auction_bids table") }
                        };
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
                "  Bid #{}: {} mwh at {} per mwh from {}",
                i + 1, bid.quantity_mwh, bid.price_per_mwh, bid.bidder
            );
        }
        
        println!("Offers: {}", orders.offers.len());
        for (i, offer) in orders.offers.iter().enumerate() {
            println!(
                "  Offer #{}: {} mwh at {} per mwh from {}",
                i + 1, offer.quantity_mwh, offer.price_per_mwh, offer.seller
            );
        }
        
        // Match orders
        println!("\nMatching orders:");
        match_orders(&orders);
    }
}

fn create_supa_client() -> Postgrest {
    // Create database client
    Postgrest::new("https://qlwtplrrqsmdlxhxhphf.supabase.co/rest/v1")
    .insert_header("apikey", dotenv::var("SUPABASE_PUBLIC_API_KEY").unwrap())
    .insert_header("Authorization", format!("Bearer {}", dotenv::var("SUPABASE_SERVICE_KEY").unwrap()))
}

#[tokio::main]
async fn update_db_auction_active(auction_id: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { 
    let supa_resp = async {
        println!("Writing updated status to Supabase...");

        let supa_client = create_supa_client();
        let resp = supa_client
            .from("auctions")
            .eq("id", auction_id)
            .update("{\"status\": \"active\"}")
            .execute()
            .await?;
        println!("Response from Supabase: {}", resp.text().await?);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    
    // Now you can just await it directly
    supa_resp.await?;
    
    Ok(())
}

#[tokio::main]
async fn add_bid_to_db(auction_id: String, bid: EnergyBid) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let supa_resp = async {
        println!("Writing new bid to Supabase...");

        let supa_client = create_supa_client();
        let bidder = bid.bidder.clone();
        let quantity_mwh = bid.quantity_mwh.clone();
        let price_per_mwh = bid.price_per_mwh.clone();

        let new_bid = format!("{{
            \"auction_id\": \"{auction_id}\",
            \"user_id\": \"{bidder}\",
            \"order_type\": \"bid\",
            \"quantity_mwh\": \"{quantity_mwh}\",
            \"price_per_mwh\": \"{price_per_mwh}\"
        }}");

        let resp = supa_client
            .from("auction_orders")
            .insert(new_bid)
            .execute()
            .await?;
        println!("Response from Supabase: {}", resp.text().await?);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    
    // Now you can just await it directly
    supa_resp.await?;
    
    Ok(())
}

#[tokio::main]
async fn add_offer_to_db(auction_id: String, offer: EnergyOffer) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let supa_resp = async {
        println!("Writing new bid to Supabase...");

        let supa_client = create_supa_client();
        let seller = offer.seller.clone();
        let quantity_mwh = offer.quantity_mwh.clone();
        let price_per_mwh = offer.price_per_mwh.clone();

        let new_offer = format!("{{
            \"auction_id\": \"{auction_id}\",
            \"user_id\": \"{seller}\",
            \"order_type\": \"bid\",
            \"quantity_mwh\": \"{quantity_mwh}\",
            \"price_per_mwh\": \"{price_per_mwh}\"
        }}");

        let resp = supa_client
            .from("auction_orders")
            .insert(new_offer)
            .execute()
            .await?;
        println!("Response from Supabase: {}", resp.text().await?);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    
    // Now you can just await it directly
    supa_resp.await?;
    
    Ok(())
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
                    let mut orders: std::sync::MutexGuard<'_, MatchOrders> = orders.lock().unwrap();
                    orders.add_bid(bid);
                    let response = serde_json::to_string(&Message::MatchResult).unwrap();
                    stream.write_all(response.as_bytes()).unwrap();
                    return "bid";
                }
                Ok(Message::Offer(offer)) => {
                    println!("Received offer: {:?}", offer);
                    let mut orders: std::sync::MutexGuard<'_, MatchOrders> = orders.lock().unwrap();
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

fn match_orders(orders: &MatchOrders) {
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
            bidder: "223d54c9-0e14-4399-9cd1-961faea299dc".to_string(),
            price_per_mwh: 100,
            quantity_mwh: 50,
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
            price_per_mwh: 90,
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
}