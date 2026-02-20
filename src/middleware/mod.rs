//! Middleware module — typed adapter with user-defined hooks.
//!
//! Provides [`Middleware`] trait and [`TypedAdapter`] wrapper for enriching
//! records on read, extracting metadata on write, filtering queries by
//! metadata, and controlling sync state reset.

pub mod types;
pub mod typed_adapter;

pub use types::Middleware;
pub use typed_adapter::{
    MiddlewareBatchResult, MiddlewareQueryResult, MiddlewarePatchManyResult, TypedAdapter,
};
