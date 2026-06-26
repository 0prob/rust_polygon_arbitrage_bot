#![allow(
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::struct_excessive_bools,
    clippy::if_not_else,
    clippy::implicit_hasher,
    clippy::items_after_statements,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::return_self_not_must_use,
    clippy::manual_checked_ops,
    clippy::unnecessary_wraps,
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::unreadable_literal,
    clippy::unnested_or_patterns,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::ignored_unit_patterns,
    clippy::used_underscore_binding
)]

pub mod abis;
pub mod config;
pub mod debug_agent;
pub mod core;
pub mod error;
pub mod infra;
pub mod orchestrator;
pub mod pipeline;
pub mod services;
#[cfg(feature = "tui")]
pub mod tui;
pub mod util;
