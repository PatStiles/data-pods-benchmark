/// Secure channel between a client and an enclave or two enclaves
mod encryption;

#[cfg(feature = "use-tokio")]
mod client;

#[cfg(feature = "use-tokio")]
pub use client::*;

mod server;
pub use server::*;
