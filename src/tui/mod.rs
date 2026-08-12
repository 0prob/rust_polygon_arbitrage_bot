pub mod app;
pub mod bridge;
pub mod demo;
pub mod events;
pub mod layout;
pub mod route_viz;
pub mod run;
pub mod terminal;
pub mod theme;
pub mod update;
pub mod widgets;

pub use app::{App, DashboardSnapshot, Tab};
pub use bridge::TuiBridge;
pub use demo::run_tui_demo;
pub use run::run_tui;
