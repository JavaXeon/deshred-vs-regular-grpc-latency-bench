use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures::{StreamExt};
use solana_sdk::signature::Signature;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{Barrier, RwLock};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateDeshred, subscribe_update::UpdateOneof as GrpcUpdateOneof, subscribe_update_deshred::UpdateOneof as DeshredUpdateOneof
};

pub static TEST_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");

#[derive(Parser, Debug)]
#[command(name = "measure_deshred_vs_regular_grpc_latency", version = "1.0", about, long_about = None)]
struct Args {
    #[arg(long)]
    url: String,

    #[arg(long)]
    token: String,

    #[arg(long, default_value_t = 30)]
    time: u64,
}

#[derive(Clone)]
#[allow(dead_code)]
struct TransactionTimestamp {
    signature: Signature,
    deshred_timestamp: Option<i64>,
    regular_timestamp: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {

    let args = Args::parse();

    let url = args.url;
    let token = args.token;

    let ready_barrier = Arc::new(Barrier::new(2));
    let tx_data: Arc<RwLock<HashMap<Signature, TransactionTimestamp>>> = Arc::new(RwLock::new(HashMap::new()));

    let tx_data_clone = tx_data.clone();
    let ready_barrier_clone = ready_barrier.clone();
    let deshred_url = url.clone();
    let deshred_token = token.clone();
    tokio::spawn(async move {
        if let Err(e) = start_deshred_stream(
            deshred_url,
            deshred_token,
            tx_data_clone,
            ready_barrier_clone,
        )
        .await
        {
            eprintln!("Error in deshred gRPC stream: {}", e);
        }
    });

    let tx_data_clone = tx_data.clone();
    let ready_barrier_clone = ready_barrier.clone();
    let regular_url = url;
    let regular_token = token;
    tokio::spawn(async move {
        if let Err(e) = start_regular_grpc_stream(
            regular_url,
            regular_token,
            tx_data_clone,
            ready_barrier_clone,
        )
        .await
        {
            eprintln!("Error in regular gRPC stream: {}", e);
        }
    });

    println!("Measuring deshred and regular gRPC latency ({} seconds)...", args.time);

    tokio::time::sleep(tokio::time::Duration::from_secs(args.time)).await;

    print_latency_statistics(&tx_data).await;

    Ok(())
}

async fn start_deshred_stream(
    url: String,
    token: String,
    tx_data: Arc<RwLock<HashMap<Signature, TransactionTimestamp>>>,
    ready_barrier: Arc<Barrier>,
) -> Result<(), anyhow::Error> {
    let mut grpc_client = GeyserGrpcClient::build_from_shared(url)?
        .x_token(Some(token))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .http2_adaptive_window(true)
        .initial_connection_window_size(8 * 1024 * 1024) // 8 MB
        .initial_stream_window_size(4 * 1024 * 1024) // 4 MB
        .connect()
        .await?;

    let request = create_deshred_subscribe_request();        
    let (_, mut stream) = grpc_client.subscribe_deshred_with_request(Some(request)).await.map_err(|e| anyhow::anyhow!(e))?;

    ready_barrier.wait().await;

    while let Some(msg) = stream.next().await {
        let Ok(update) = msg else { break };
        handle_deshred_update(&update, tx_data.clone()).await?;
    }

    Ok(())
}

async fn start_regular_grpc_stream(
    url: String,
    token: String,
    tx_data: Arc<RwLock<HashMap<Signature, TransactionTimestamp>>>,
    ready_barrier: Arc<Barrier>,
) -> Result<(), anyhow::Error> {
    let mut grpc_client = GeyserGrpcClient::build_from_shared(url)?
        .x_token(Some(token))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    let request = create_grpc_subscribe_request();        
    let (_, mut stream) = grpc_client.subscribe_with_request(Some(request)).await.map_err(|e| anyhow::anyhow!(e))?;

    ready_barrier.wait().await;

    while let Some(msg) = stream.next().await {
        let Ok(update) = msg else { break };
        handle_regular_update(&update, tx_data.clone()).await?;
    }

    Ok(())
}

async fn handle_deshred_update(
    update: &SubscribeUpdateDeshred,
    tx_data: Arc<RwLock<HashMap<Signature, TransactionTimestamp>>>,
) -> Result<(), anyhow::Error> {
    match &update.update_oneof {
        Some(DeshredUpdateOneof::DeshredTransaction(txn)) => {

            let timestamp_ns = current_timestamp_ns()?;            

            let tx = txn.transaction.as_ref().ok_or_else(|| anyhow::anyhow!("Missing transaction data"))?;
            let signature = Signature::try_from(tx.signature.as_slice())?;

            let mut data = tx_data.write().await;
            data.entry(signature.clone())
                .and_modify(|entry| {
                    entry.deshred_timestamp.get_or_insert(timestamp_ns);
                })
                .or_insert(TransactionTimestamp {
                    signature,
                    deshred_timestamp: Some(timestamp_ns),
                    regular_timestamp: None,
                });
        }
        Some(DeshredUpdateOneof::Ping(_)) => {}
        _ => {}
    }

    Ok(())
}

async fn handle_regular_update(
    update: &SubscribeUpdate,
    tx_data: Arc<RwLock<HashMap<Signature, TransactionTimestamp>>>,
) -> Result<(), anyhow::Error> {
    match &update.update_oneof {
        Some(GrpcUpdateOneof::Transaction(txn)) => {

            let timestamp_ns = current_timestamp_ns()?;

            let tx = txn.transaction.as_ref().ok_or_else(|| anyhow::anyhow!("Missing transaction data"))?;
            let signature = Signature::try_from(tx.signature.as_slice())?;

            let mut data = tx_data.write().await;
            data.entry(signature.clone())
                .and_modify(|entry| {
                    entry.regular_timestamp.get_or_insert(timestamp_ns);
                })
                .or_insert(TransactionTimestamp {
                    signature,
                    deshred_timestamp: None,
                    regular_timestamp: Some(timestamp_ns),
                });
        }
        Some(GrpcUpdateOneof::Ping(_)) => {}
        _ => {}
    }

    Ok(())
}

fn create_deshred_subscribe_request() -> SubscribeDeshredRequest {
    let mut transactions_filter = HashMap::new();
    transactions_filter.insert(
        "test".to_string(),
        SubscribeRequestFilterDeshredTransactions {
            account_include: vec![TEST_PROGRAM_ID.to_string()],
            vote: Some(false),
            ..Default::default()
        },
    );

    SubscribeDeshredRequest {
        deshred_transactions: transactions_filter,
        ..Default::default()
    }
}

fn create_grpc_subscribe_request() -> SubscribeRequest {
    let mut transactions_filter = HashMap::new();
    transactions_filter.insert(
        "test".to_string(),
        SubscribeRequestFilterTransactions {
            account_include: vec![TEST_PROGRAM_ID.to_string()],
            vote: Some(false),
            failed: Some(true),
            ..Default::default()
        },
    );

    SubscribeRequest {
        slots: HashMap::new(),
        blocks_meta: HashMap::new(),
        transactions: transactions_filter,
        accounts: HashMap::new(),
        commitment: Some(0),
        ..Default::default()
    }
}

fn percentile_sorted_ns(sorted_values: &[i64], percentile: f64) -> i64 {
    if sorted_values.is_empty() {
        return 0;
    }

    let n = sorted_values.len();
    let rank = (percentile * n as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(n - 1);
    sorted_values[index]
}

fn current_timestamp_ns() -> Result<i64, anyhow::Error> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow::anyhow!(e))?
        .as_nanos() as i64)
}

async fn print_latency_statistics(tx_data: &Arc<RwLock<HashMap<Signature, TransactionTimestamp>>>) {
    let data = tx_data.read().await;

    let mut latencies: Vec<i64> = Vec::new();
    let mut deshred_total = 0usize;
    let mut regular_total = 0usize;
    let mut matched_count = 0usize;
    let mut deshred_only = 0usize;
    let mut regular_only = 0usize;
    let mut deshred_earlier_count = 0usize;
    let mut regular_earlier_count = 0usize;
    let mut same_time_count = 0usize;

    for tx in data.values() {
        let has_deshred = tx.deshred_timestamp.is_some();
        let has_regular = tx.regular_timestamp.is_some();

        if has_deshred {
            deshred_total += 1;
        }

        if has_regular {
            regular_total += 1;
        }

        match (tx.deshred_timestamp, tx.regular_timestamp) {
            (Some(deshred_ts), Some(regular_ts)) => {
                let latency_ns = regular_ts - deshred_ts;
                latencies.push(latency_ns);
                matched_count += 1;

                if latency_ns > 0 {
                    deshred_earlier_count += 1;
                } else if latency_ns < 0 {
                    regular_earlier_count += 1;
                } else {
                    same_time_count += 1;
                }
            }
            (Some(_), None) => deshred_only += 1,
            (None, Some(_)) => regular_only += 1,
            (None, None) => {}
        }
    }

    println!("\n=== Latency Statistics (regular - deshred) ===");
    println!("Unique signatures tracked: {}", data.len());
    println!("Deshred total received: {}", deshred_total);
    println!("Regular gRPC total received: {}", regular_total);
    println!("Matched transactions (both sources): {}", matched_count);
    println!("Unmatched deshred-only: {}", deshred_only);
    println!("Unmatched regular-only: {}", regular_only);

    if matched_count > 0 {
        let matched_total = matched_count as f64;
        let deshred_earlier_pct = deshred_earlier_count as f64 * 100.0 / matched_total;
        let regular_earlier_pct = regular_earlier_count as f64 * 100.0 / matched_total;
        let same_time_pct = same_time_count as f64 * 100.0 / matched_total;

        println!("Deshred arrived earlier: {} ({:.2}%)", deshred_earlier_count, deshred_earlier_pct);
        println!("Regular gRPC arrived earlier: {} ({:.2}%)", regular_earlier_count, regular_earlier_pct);
        println!("Arrived at the same time: {} ({:.2}%)", same_time_count, same_time_pct);
    }

    if latencies.is_empty() {
        println!("(Waiting for transactions to arrive from both sources...)\n");
        return;
    }

    latencies.sort();
    let min_latency_ns = latencies[0];
    let max_latency_ns = latencies[latencies.len() - 1];
    let avg_latency_ns = latencies.iter().sum::<i64>() / latencies.len() as i64;
    let median_latency_ns = latencies[latencies.len() / 2];
    let p50_latency_ns = percentile_sorted_ns(&latencies, 0.50);
    let p90_latency_ns = percentile_sorted_ns(&latencies, 0.90);
    let p99_latency_ns = percentile_sorted_ns(&latencies, 0.99);

    println!("Min latency: {:.3} ms", min_latency_ns as f64 / 1_000_000.0);
    println!("Max latency: {:.3} ms", max_latency_ns as f64 / 1_000_000.0);
    println!("Avg latency: {:.3} ms", avg_latency_ns as f64 / 1_000_000.0);
    println!("Median latency: {:.3} ms", median_latency_ns as f64 / 1_000_000.0);
    println!("P50 latency: {:.3} ms", p50_latency_ns as f64 / 1_000_000.0);
    println!("P90 latency: {:.3} ms", p90_latency_ns as f64 / 1_000_000.0);
    println!("P99 latency: {:.3} ms", p99_latency_ns as f64 / 1_000_000.0);
    println!("Positive value = regular arrived later / deshred arrived earlier");
    println!("Negative value = regular arrived earlier / deshred arrived later\n");
}
