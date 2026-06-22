use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::tui::app::{App, Tab};

pub fn handle_key(app: &mut App, key: KeyEvent) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.should_quit = true;
        return true;
    }

    if app.show_help {
        app.show_help = false;
        return true;
    }

    if app.filter.editing {
        return handle_filter_input(app, key);
    }

    match key.code {
        KeyCode::Char('q') => {
            app.should_quit = true;
        }
        KeyCode::Char('?') => app.show_help = true,
        KeyCode::Char('/') => {
            app.filter.editing = true;
        }
        KeyCode::Char(c @ '1'..='8') => {
            app.tab = Tab::from_index((c as u8 - b'1') as usize);
        }
        KeyCode::Tab => {
            let next = (app.tab.index() + 1) % Tab::ALL.len();
            app.tab = Tab::from_index(next);
        }
        KeyCode::BackTab => {
            let prev = app.tab.index().checked_sub(1).unwrap_or(Tab::ALL.len() - 1);
            app.tab = Tab::from_index(prev);
        }
        KeyCode::Char('d') if app.tab == Tab::Opportunities => {
            app.show_detail = !app.show_detail;
        }
        KeyCode::Char('f') if app.tab == Tab::Opportunities => {
            app.filter.hop_stratified = !app.filter.hop_stratified;
            if app.filter.hop_stratified {
                app.filter.min_hops = 2;
                app.filter.max_hops = 5;
            }
        }
        KeyCode::Char('l') if app.tab == Tab::Opportunities => {
            app.filter.long_tail_only = !app.filter.long_tail_only;
            app.clamp_selection();
        }
        KeyCode::Up | KeyCode::Char('k') => {
            if app.tab == Tab::Opportunities && app.opp_selected > 0 {
                app.opp_selected -= 1;
                app.clamp_selection();
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.tab == Tab::Opportunities {
                app.opp_selected += 1;
                app.clamp_selection();
            }
        }
        KeyCode::Enter if app.tab == Tab::Opportunities => {
            app.show_detail = !app.show_detail;
        }
        KeyCode::Esc => {
            app.filter.text.clear();
            app.show_detail = false;
        }
        _ => {}
    }
    true
}

fn handle_filter_input(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc => {
            app.filter.editing = false;
        }
        KeyCode::Enter => {
            app.filter.editing = false;
            app.opp_selected = 0;
            app.clamp_selection();
        }
        KeyCode::Backspace => {
            app.filter.text.pop();
        }
        KeyCode::Char(c) => {
            app.filter.text.push(c);
            app.opp_selected = 0;
            app.clamp_selection();
        }
        _ => {}
    }
    true
}
