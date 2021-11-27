pub mod indexer;
pub mod data;

/// index healthcheck status.
#[derive(Debug)]
pub enum IndexStatus {
    New,
    Indexed,
    Incomplete,
    Corrupted,
    Indexing
}