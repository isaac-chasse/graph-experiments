use csv::ReaderBuilder;
use std::{collections::HashMap, fs::File, io::BufReader, sync::{Arc, Mutex}, u32, usize, time::Duration};

use tracing::{info, warn};

// enum Action {
//     AddEdge,
//     RemoveEdge,
// }
//
// struct QueueItem {
//     action: Action,
//     source_node: u32,
//     target_node: u32,
// }

#[derive(Clone)]
struct NodeMap {
    outgoing_edges: Vec<u32>,
    incoming_edges: Vec<u32>,
}

struct Graph {
    nodes: HashMap<u32, NodeMap>,
    // next_node_id: u32,
    // pending_queue: Vec<QueueItem>,
    // is_loaded: bool,
}

impl Graph {
    fn new(expected_node_count: u32) -> Self {
        Graph {
            nodes: HashMap::with_capacity(expected_node_count as usize),
            // next_node_id: 0,
            // pending_queue: Vec::new(),
            // is_loaded: false,
        }
    }

    fn add_edge(&mut self, source: u32, target: u32) {
        let mut nodes = self.nodes.clone();
        let source_map = nodes.entry(source).or_insert_with(|| NodeMap {
            outgoing_edges: Vec::new(),
            incoming_edges: Vec::new(),
        });
        source_map.outgoing_edges.push(target);

        let target_map = nodes.entry(target).or_insert_with(|| NodeMap {
            outgoing_edges: Vec::new(),
            incoming_edges: Vec::new(),
        });
        target_map.incoming_edges.push(source);
    }
}

impl Graph {
    fn load_from_tsv(&mut self, path: &str) -> std::io::Result<()> {
        info!("attempting to load");
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut rows = ReaderBuilder::new().delimiter(b'\t').from_reader(reader);
        let mut rec = csv::StringRecord::new();
        let mut row_count = 0;

        while rows.read_record(&mut rec)? {
            row_count += 1;

            if row_count % 1_000 == 0 {
                info!("Processed {} rows", row_count);
            }

            let source: u32 = rec.get(0).unwrap().parse().unwrap();
            let target: u32 = rec.get(1).unwrap().parse().unwrap();

            self.add_edge(source, target);
        }

        info!("Loaded graph with {}", row_count); // should be user count

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // initialize tracing subscriber
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // set up port
    let port = std::env::var("PORT").unwrap_or("8000".to_string());

    // grab benchmark csv
    let csv_path = std::env::var("CSV_BENCHMARK").unwrap_or(
        "/Users/isaac.chasse/coding/rustlang/graph-experiments/data/benches/ca-HepPh_adj.tsv"
            .to_string(),
    );
    let expected_node_count = std::env::var("EXPECTED_NODE_COUNT")
        .unwrap_or("5000000".to_string())
        .parse::<u32>()
        .unwrap();

    info!("Bench: {}", csv_path);
    info!(
        "Memgraph started on port {} with node capacity of {}",
        port, expected_node_count
    );
    info!("Starting up!");

    let graph = Graph::new(expected_node_count);
    let graph = Arc::new(Mutex::new(graph));

    let graph_clone = graph.clone();
    let csv_path_clone = csv_path.clone();

    tokio::spawn(async move {
        info!("In here");
        let mut graph_clone = graph_clone.lock().unwrap();
        match graph_clone.load_from_tsv(&csv_path_clone) {
            Ok(_) => info!("Loaded graph from CSV"),
            Err(e) => warn!("Failed to load graph from CSV: {}", e),
        }
    })
    .await
    .expect("Failed to spawn task");

    tokio::time::sleep(Duration::from_secs(5)).await;
}
