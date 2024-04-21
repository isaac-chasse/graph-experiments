use dotenv::dotenv;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

mod graph;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let project_path = std::env::var("PROJECT_PATH").expect("Expected a project path env var");

    // initialize tracing subscriber
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // set up port
    let port = std::env::var("PORT").unwrap_or("8000".to_string());

    // grab benchmark csv
    let mut csv_path =
        std::env::var("BENCHMARK_PATH").expect("Expected benchmark dataset in env var");
    csv_path = format!("{}{}", project_path, csv_path);
    let expected_node_count = std::env::var("EXPECTED_NODE_COUNT")
        .unwrap_or("5000000".to_string())
        .parse::<u32>()
        .unwrap();

    info!(
        "Memgraph started on port {} with node capacity of {}",
        port, expected_node_count
    );
    info!("Starting up!");

    let graph = graph::Graph::new(expected_node_count);
    let graph = Arc::new(Mutex::new(graph));

    let graph_clone = graph.clone();
    let csv_path_clone = csv_path.clone();

    tokio::spawn(async move {
        let mut graph_clone = graph_clone.lock().unwrap();
        match graph_clone.load_from_tsv(&csv_path_clone) {
            Ok(_) => info!("Loaded graph from CSV"),
            Err(e) => warn!("Failed to load graph from CSV: {}", e),
        }
    })
    .await
    .expect("Failed to spawn task");
}
