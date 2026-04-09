//! Sales-only kernel for the PulsivoSalesman daemon.

pub mod config;
pub mod config_reload;
pub mod error;
pub mod kernel;
pub mod registry;
pub mod supervisor;

pub use kernel::PulsivoSalesmanKernel;
