use csv::ReaderBuilder;
use std::{collections::HashMap, fs::File, io::BufReader};
use tracing::info;

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
pub struct NodeMap {
    pub outgoing_edges: Vec<u32>,
    pub incoming_edges: Vec<u32>,
}

pub struct Graph {
    pub nodes: HashMap<u32, NodeMap>,
    // next_node_id: u32,
    // pending_queue: Vec<QueueItem>,
    // is_loaded: bool,
}

impl Graph {
    pub fn new(expected_node_count: u32) -> Self {
        Graph {
            nodes: HashMap::with_capacity(expected_node_count as usize),
            // next_node_id: 0,
            // pending_queue: Vec::new(),
            // is_loaded: false,
        }
    }

    pub fn add_edge(&mut self, source: u32, target: u32) {
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
    pub fn load_from_tsv(&mut self, path: &str) -> std::io::Result<()> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut rows = ReaderBuilder::new().delimiter(b'\t').from_reader(reader);
        let mut rec = csv::StringRecord::new();
        let mut row_count = 0;

        while rows.read_record(&mut rec)? {
            row_count += 1;

            if row_count % 100_000 == 0 {
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
