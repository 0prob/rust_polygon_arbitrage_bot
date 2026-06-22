use ratatui::style::{Color, Modifier, Style};

pub struct Theme;

impl Theme {
    pub fn bg() -> Color {
        Color::Rgb(15, 17, 23)
    }

    pub fn fg() -> Color {
        Color::Rgb(220, 223, 228)
    }

    pub fn header() -> Style {
        Style::default()
            .fg(Color::Rgb(180, 190, 210))
            .add_modifier(Modifier::BOLD)
    }

    pub fn title() -> Style {
        Style::default()
            .fg(Color::Rgb(120, 180, 255))
            .add_modifier(Modifier::BOLD)
    }

    pub fn profit() -> Style {
        Style::default().fg(Color::Rgb(80, 220, 120))
    }

    pub fn loss() -> Style {
        Style::default().fg(Color::Rgb(240, 90, 90))
    }

    pub fn warn() -> Style {
        Style::default().fg(Color::Rgb(240, 180, 60))
    }

    pub fn muted() -> Style {
        Style::default().fg(Color::Rgb(120, 125, 135))
    }

    pub fn accent() -> Style {
        Style::default().fg(Color::Rgb(140, 200, 255))
    }

    pub fn tab_active() -> Style {
        Style::default()
            .fg(Color::Rgb(255, 255, 255))
            .bg(Color::Rgb(40, 70, 110))
            .add_modifier(Modifier::BOLD)
    }

    pub fn tab_inactive() -> Style {
        Style::default().fg(Color::Rgb(140, 145, 155))
    }

    pub fn block_border() -> Style {
        Style::default().fg(Color::Rgb(60, 70, 90))
    }

    pub fn selected_row() -> Style {
        Style::default()
            .bg(Color::Rgb(35, 45, 65))
            .add_modifier(Modifier::BOLD)
    }

    pub fn protocol_badge() -> Style {
        Style::default()
            .fg(Color::Rgb(200, 210, 255))
            .bg(Color::Rgb(45, 50, 75))
    }

    pub fn long_tail() -> Style {
        Style::default()
            .fg(Color::Rgb(255, 160, 80))
            .add_modifier(Modifier::ITALIC)
    }

    pub fn score_style(score: f64) -> Style {
        if score < -0.001 {
            Self::profit()
        } else if score > 0.0 {
            Self::loss()
        } else {
            Self::muted()
        }
    }

    pub fn status_style(status: crate::tui::app::BotStatus) -> Style {
        match status {
            crate::tui::app::BotStatus::Scanning => Self::accent(),
            crate::tui::app::BotStatus::Executing => Self::warn(),
            crate::tui::app::BotStatus::Error => Self::loss(),
            crate::tui::app::BotStatus::Mock => Style::default().fg(Color::Magenta),
            _ => Self::muted(),
        }
    }
}
