//! Ratatui dashboard for the polarb arbitrage bot.
//!
//! Run with: `cargo run --bin tui --features tui`
//! Mock demo: `cargo run --bin tui --features tui -- --mock`

pub mod app;
pub mod bridge;
pub mod events;
pub mod hook;
pub mod layout;
pub mod mock;
pub mod route_viz;
pub mod run;
pub mod text;
pub mod theme;
pub mod update;
pub mod widgets;

pub use app::{App, Tab};
pub use bridge::UiBridge;
pub use hook::TuiUiHook;
pub use run::run_tui;
pub use update::UiUpdate;
