//! Shared types for the sales-only PulsivoSalesman daemon.
//!
//! This crate defines the data structures used across the sales kernel, runtime,
//! API surface, and memory substrate. It contains no business logic.

pub mod agent;
pub mod capability;
pub mod config;
pub mod error;
pub mod event;
pub mod manifest_signing;
pub mod memory;
pub mod message;
pub mod model_catalog;
pub mod serde_compat;
pub mod taint;
pub mod tool;
