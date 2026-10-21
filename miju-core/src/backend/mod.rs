//! Core traits and types for interacting with backends
//!
//! # Overview
//! - [`Codec`](codec::Codec): Converts values between a type and its encoded representation.
//! - [`Queue`](queue::Queue): Represents a queue in the backend.
//! - [`TaskResult`]: Represents the result of a task execution.

pub mod codec;
pub mod queue;
mod results;

pub use results::*;
