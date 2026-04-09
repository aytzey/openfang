//! Kernel-specific error types.

use pulsivo_salesman_types::error::PulsivoSalesmanError;
use thiserror::Error;

/// Kernel error type wrapping PulsivoSalesmanError with kernel-specific context.
#[derive(Error, Debug)]
pub enum KernelError {
    /// A wrapped PulsivoSalesmanError.
    #[error(transparent)]
    PulsivoSalesman(#[from] PulsivoSalesmanError),

    /// The kernel failed to boot.
    #[error("Boot failed: {0}")]
    BootFailed(String),
}

/// Alias for kernel results.
pub type KernelResult<T> = Result<T, KernelError>;
