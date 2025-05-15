use crate::EnergyBid;
use crate::EnergyOffer;
use postgrest::Postgrest;
use serde::{Serialize, Deserialize};
use std::fmt;

#[derive(Debug)]
pub enum DbError {
    EnvError(std::env::VarError),
    RequestError(Box<dyn std::error::Error + Send + Sync>),
    SerializationError(serde_json::Error),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::EnvError(e) => write!(f, "Environment variable error: {}", e),
            DbError::RequestError(e) => write!(f, "Database request error: {}", e),
            DbError::SerializationError(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl std::error::Error for DbError {}

impl From<std::env::VarError> for DbError {
    fn from(error: std::env::VarError) -> Self {
        DbError::EnvError(error)
    }
}

impl From<serde_json::Error> for DbError {
    fn from(error: serde_json::Error) -> Self {
        DbError::SerializationError(error)
    }
}

impl From<reqwest::Error> for DbError {
    fn from(error: reqwest::Error) -> Self {
        DbError::RequestError(Box::new(error))
    }
}

impl From<std::io::Error> for DbError {
    fn from(error: std::io::Error) -> Self {
        DbError::RequestError(Box::new(error))
    }
}

pub fn from_error<E>(error: E) -> DbError 
where 
    E: std::error::Error + Send + Sync + 'static
{
    DbError::RequestError(Box::new(error))
}

impl From<dotenv::Error> for DbError {
    fn from(error: dotenv::Error) -> Self {
        DbError::RequestError(Box::new(error))
    }
}

pub fn create_client() -> Result<Postgrest, DbError> {
    let public_key = dotenv::var("SUPABASE_PUBLIC_API_KEY")?;
    let service_key = dotenv::var("SUPABASE_SERVICE_KEY")?;
    
    Ok(Postgrest::new("https://qlwtplrrqsmdlxhxhphf.supabase.co/rest/v1")
        .insert_header("apikey", public_key)
        .insert_header("Authorization", format!("Bearer {}", service_key)))
}

#[derive(Debug, Serialize, Deserialize)]
struct AuctionOrder {
    auction_id: String,
    user_id: String,
    order_type: String,
    quantity_mwh: u64,
    price_per_mwh: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct AuctionResult {
    auction_id: String,
    clearing_price: u64,
    participant_count: u64,
    cleared_money_volume: u64,
    cleared_mwh_volume: u64
}

#[tokio::main]
pub async fn update_auction_status(auction_id: &str, status: &str) -> Result<(), DbError> {
    println!("Updating auction status to '{}' in Supabase...", status);
    
    let client = create_client()?;
    let update_data = serde_json::json!({ "status": status });
    
    let resp = match client
        .from("auctions")
        .eq("id", auction_id)
        .update(update_data.to_string())
        .execute()
        .await {
            Ok(resp) => resp,
            Err(e) => return Err(from_error(e)),
        };
        
    match resp.text().await {
        Ok(text) => println!("Response from Supabase: {}", text),
        Err(e) => eprintln!("Failed to get response text: {}", e),
    }
    Ok(())
}

#[tokio::main]
pub async fn update_order_status(auction_id: &str, user_ids: Vec<&str>, status: &str) -> Result<(), DbError> {
    println!("Updating order status for users to '{}' in Supabase...", status);
    
    let client = create_client()?;
    let update_data = serde_json::json!({ "status": status });
    
    let resp = match client
        .from("auction_orders")
        .eq("auction_id", auction_id)
        .in_("user_id", user_ids)
        .update(update_data.to_string())
        .execute()
        .await {
            Ok(resp) => resp,
            Err(e) => return Err(from_error(e)),
        };
        
    match resp.text().await {
        Ok(text) => println!("Response from Supabase: {}", text),
        Err(e) => eprintln!("Failed to get response text: {}", e),
    }
    Ok(())
}

#[tokio::main]
async fn add_order(order: AuctionOrder) -> Result<(), DbError> {
    println!("Writing new {} to Supabase...", order.order_type);
    
    let client = create_client()?;
    let order_json = serde_json::to_string(&order)?;
    
    let resp = match client
        .from("auction_orders")
        .insert(order_json)
        .execute()
        .await {
            Ok(resp) => resp,
            Err(e) => return Err(from_error(e)),
        };
        
        match resp.text().await {
            Ok(text) => println!("Response from Supabase: {}", text),
            Err(e) => eprintln!("Failed to get response text: {}", e),
        }
        Ok(())
}

pub fn add_bid(auction_id: &str, bid: &EnergyBid) -> Result<(), DbError> {
    let order = AuctionOrder {
        auction_id: auction_id.to_string(),
        user_id: bid.bidder.clone(),
        order_type: "bid".to_string(),
        quantity_mwh: bid.quantity_mwh,
        price_per_mwh: bid.price_per_mwh,
    };
    
    add_order(order)
}

pub fn add_offer(auction_id: &str, offer: &EnergyOffer) -> Result<(), DbError> {
    let order = AuctionOrder {
        auction_id: auction_id.to_string(),
        user_id: offer.seller.clone(),
        order_type: "offer".to_string(),
        quantity_mwh: offer.quantity_mwh,
        price_per_mwh: offer.price_per_mwh,
    };
    
    add_order(order)
}

#[tokio::main]
pub async fn add_auction_result(
    auction_id: &str,
    clearing_price: &u64,
    participant_count: &u64,
    cleared_money_volume: &u64,
    cleared_mwh_volume: &u64
) -> Result<(), DbError> {
    println!("Adding result of auction '{}' in Supabase...", auction_id);

    let result = AuctionResult {
        auction_id: auction_id.to_string(),
        clearing_price: clearing_price.clone(),
        participant_count: participant_count.clone(),
        cleared_money_volume: cleared_money_volume.clone(),
        cleared_mwh_volume: cleared_mwh_volume.clone()
    };
    let result_json = serde_json::to_string(&result)?;

    let client = create_client()?;
    
    let resp = match client
        .from("auction_results")
        .insert(result_json)
        .execute()
        .await {
            Ok(resp) => resp,
            Err(e) => return Err(from_error(e)),
        };
        
    match resp.text().await {
        Ok(text) => println!("Response from Supabase: {}", text),
        Err(e) => eprintln!("Failed to get response text: {}", e),
    }
    Ok(())
}
