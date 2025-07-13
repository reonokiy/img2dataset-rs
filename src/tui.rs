use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{BarChart, Block, Borders, Gauge, List, ListItem, Paragraph, Row, Table, Tabs},
};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc};

#[derive(Debug, Clone)]
pub struct DownloadStats {
    pub downloading: u64,
    pub success: u64,
    pub failed: u64,
    pub total_processed: u64,
    pub current_shard: u64,
    pub memory_usage_mb: f64,
    pub download_rate: f64, // downloads per second
    pub errors: Vec<String>,
    pub start_time: Instant,
}

impl Default for DownloadStats {
    fn default() -> Self {
        Self {
            downloading: 0,
            success: 0,
            failed: 0,
            total_processed: 0,
            current_shard: 0,
            memory_usage_mb: 0.0,
            download_rate: 0.0,
            errors: Vec::new(),
            start_time: Instant::now(),
        }
    }
}

pub struct TuiApp {
    stats: Arc<RwLock<DownloadStats>>,
    cached_stats: DownloadStats, // Cache last known stats
    should_quit: bool,
    selected_tab: usize,
}

impl TuiApp {
    pub fn new(stats: Arc<RwLock<DownloadStats>>) -> Self {
        Self {
            stats,
            cached_stats: DownloadStats::default(),
            should_quit: false,
            selected_tab: 0,
        }
    }

    pub fn run<B: ratatui::backend::Backend>(
        &mut self,
        terminal: &mut Terminal<B>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            // Try to update cached stats - this should happen frequently
            if let Ok(stats) = self.stats.try_read() {
                self.cached_stats = stats.clone();
            }
            // Note: If we can't get the lock, we'll just use the last cached stats
            // This prevents the UI from going blank

            // Always redraw the terminal to show updated data
            match terminal.draw(|f| self.ui(f)) {
                Ok(_) => {}
                Err(e) => {
                    // Log drawing errors but continue
                    log::warn!("TUI drawing error: {}", e);
                }
            }

            // Poll for events with a shorter timeout for more responsive UI
            if event::poll(Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') => {
                            self.should_quit = true;
                        }
                        KeyCode::Char('c') => {
                            // Handle Ctrl+C
                            if key
                                .modifiers
                                .contains(crossterm::event::KeyModifiers::CONTROL)
                            {
                                self.should_quit = true;
                            }
                        }
                        KeyCode::Esc => {
                            // Also allow Escape to quit
                            self.should_quit = true;
                        }
                        KeyCode::Left => {
                            self.selected_tab = if self.selected_tab > 0 {
                                self.selected_tab - 1
                            } else {
                                2
                            };
                        }
                        KeyCode::Right => {
                            self.selected_tab = (self.selected_tab + 1) % 3;
                        }
                        _ => {}
                    }
                }
            }

            if self.should_quit {
                break;
            }

            // Small sleep to prevent excessive CPU usage
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    fn ui(&mut self, f: &mut Frame) {
        let size = f.area();

        // Ensure minimum size for proper rendering
        if size.width < 60 || size.height < 10 {
            let error_msg = Paragraph::new("Terminal too small! Minimum 60x10")
                .style(Style::default().fg(Color::Red));
            f.render_widget(error_msg, size);
            return;
        }

        // Create layout
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(3), // Tabs
                Constraint::Min(0),    // Content
            ])
            .split(size);

        // Tabs
        let titles: Vec<Line> = vec!["Overview", "Detailed Stats", "Error Logs"]
            .iter()
            .cloned()
            .map(Line::from)
            .collect();
        let tabs = Tabs::new(titles)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("img2dataset-rs TUI - Press 'q', Ctrl+C, or Esc to exit"),
            )
            .style(Style::default().fg(Color::Cyan))
            .highlight_style(
                Style::default()
                    .add_modifier(Modifier::BOLD)
                    .bg(Color::Black),
            )
            .select(self.selected_tab);
        f.render_widget(tabs, chunks[0]);

        // Content based on selected tab
        match self.selected_tab {
            0 => self.render_overview(f, chunks[1]),
            1 => self.render_detailed_stats(f, chunks[1]),
            2 => self.render_error_logs(f, chunks[1]),
            _ => {
                // Fallback content
                let error_msg =
                    Paragraph::new("Invalid tab selected").style(Style::default().fg(Color::Red));
                f.render_widget(error_msg, chunks[1]);
            }
        }
    }

    fn render_overview(&self, f: &mut Frame, area: Rect) {
        let stats = &self.cached_stats;

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Summary
                Constraint::Length(5), // Progress bars
                Constraint::Min(0),    // Charts
            ])
            .split(area);

        // Summary
        let elapsed = stats.start_time.elapsed();
        let elapsed_str = format!(
            "{}:{:02}:{:02}",
            elapsed.as_secs() / 3600,
            (elapsed.as_secs() % 3600) / 60,
            elapsed.as_secs() % 60
        );

        let summary = Paragraph::new(Text::from(vec![Line::from(vec![
            Span::styled("Runtime: ", Style::default().fg(Color::Yellow)),
            Span::raw(elapsed_str),
            Span::raw("  "),
            Span::styled("Current Shard: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{}", stats.current_shard)),
            Span::raw("  "),
            Span::styled("Memory Usage: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.1} MB", stats.memory_usage_mb)),
            Span::raw("  "),
            Span::styled("Download Rate: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:.1}/s", stats.download_rate)),
        ])]))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Status Overview"),
        );
        f.render_widget(summary, chunks[0]);

        // Progress section
        let progress_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(33),
                Constraint::Percentage(33),
                Constraint::Percentage(34),
            ])
            .split(chunks[1]);

        // Success rate
        let total = stats.success + stats.failed;
        let success_rate = if total > 0 {
            (stats.success as f64 / total as f64 * 100.0) as u16
        } else {
            0
        };

        let success_gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("Success Rate ({}/{})", stats.success, total)),
            )
            .gauge_style(Style::default().fg(Color::Green))
            .percent(success_rate);
        f.render_widget(success_gauge, progress_chunks[0]);

        // Currently downloading
        let downloading_gauge = Gauge::default()
            .block(Block::default().borders(Borders::ALL).title("Downloading"))
            .gauge_style(Style::default().fg(Color::Blue))
            .ratio(if stats.downloading > 100 {
                1.0
            } else {
                stats.downloading as f64 / 100.0
            });
        f.render_widget(downloading_gauge, progress_chunks[1]);

        // Failed count
        let failed_percentage = if total > 0 {
            (stats.failed as f64 / total as f64 * 100.0) as u16
        } else {
            0
        };
        let failed_gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("Failed ({})", stats.failed)),
            )
            .gauge_style(Style::default().fg(Color::Red))
            .percent(failed_percentage);
        f.render_widget(failed_gauge, progress_chunks[2]);

        // Chart section
        let chart_data = vec![
            ("Success", stats.success),
            ("Failed", stats.failed),
            ("In Progress", stats.downloading),
        ];

        let barchart = BarChart::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Download Statistics"),
            )
            .data(&chart_data)
            .bar_width(9)
            .bar_style(Style::default().fg(Color::Yellow))
            .value_style(Style::default().fg(Color::Black).bg(Color::Yellow));
        f.render_widget(barchart, chunks[2]);
    }

    fn render_detailed_stats(&self, f: &mut Frame, area: Rect) {
        let stats = &self.cached_stats;

        // Create owned strings to avoid borrowing issues
        let total_processed = format!("{}", stats.total_processed);
        let success = format!("{}", stats.success);
        let failed = format!("{}", stats.failed);
        let downloading = format!("{}", stats.downloading);
        let current_shard = format!("{}", stats.current_shard);
        let memory_usage = format!("{:.2} MB", stats.memory_usage_mb);
        let download_rate = format!("{:.2} items/sec", stats.download_rate);
        let runtime = format!("{:.1} seconds", stats.start_time.elapsed().as_secs_f64());

        let rows = vec![
            Row::new(vec!["Total Processed", &total_processed]),
            Row::new(vec!["Successful Downloads", &success]),
            Row::new(vec!["Failed Downloads", &failed]),
            Row::new(vec!["Currently Downloading", &downloading]),
            Row::new(vec!["Current Shard", &current_shard]),
            Row::new(vec!["Memory Usage", &memory_usage]),
            Row::new(vec!["Download Rate", &download_rate]),
            Row::new(vec!["Runtime", &runtime]),
        ];

        let table = Table::new(
            rows,
            [Constraint::Percentage(50), Constraint::Percentage(50)],
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Detailed Statistics"),
        )
        .header(
            Row::new(vec!["Metric", "Value"]).style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .widths(&[Constraint::Percentage(50), Constraint::Percentage(50)]);

        f.render_widget(table, area);
    }

    fn render_error_logs(&self, f: &mut Frame, area: Rect) {
        let stats = &self.cached_stats;

        let error_items: Vec<ListItem> = stats
            .errors
            .iter()
            .rev() // Show newest errors first
            .take(100) // Limit to last 100 errors
            .enumerate()
            .map(|(i, error)| {
                ListItem::new(Line::from(Span::styled(
                    format!("[{}] {}", i + 1, error),
                    Style::default().fg(Color::Red),
                )))
            })
            .collect();

        let error_count = error_items.len();
        let error_list = List::new(error_items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("Error Logs (Recent {})", error_count)),
            )
            .style(Style::default().fg(Color::White));

        f.render_widget(error_list, area);
    }
}

pub async fn run_tui(stats: Arc<RwLock<DownloadStats>>) -> Result<(), Box<dyn std::error::Error>> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app and run it
    let mut app = TuiApp::new(stats);
    let res = app.run(&mut terminal);

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    res
}

#[derive(Debug, Clone)]
pub enum StatsUpdate {
    DownloadStarted,
    DownloadSuccess,
    DownloadFailed(String),
    ShardChanged(u64),
    MemoryUsage(f64),
}

pub fn create_stats_updater() -> (
    Arc<RwLock<DownloadStats>>,
    mpsc::UnboundedSender<StatsUpdate>,
) {
    let stats = Arc::new(RwLock::new(DownloadStats::default()));
    let (tx, mut rx) = mpsc::unbounded_channel();

    let stats_clone = stats.clone();
    tokio::spawn(async move {
        let mut last_success_count = 0;
        let mut last_update_time = Instant::now();

        while let Some(update) = rx.recv().await {
            let mut stats = stats_clone.write().await;

            match update {
                StatsUpdate::DownloadStarted => {
                    stats.downloading += 1;
                }
                StatsUpdate::DownloadSuccess => {
                    stats.downloading = stats.downloading.saturating_sub(1);
                    stats.success += 1;
                    stats.total_processed += 1;
                }
                StatsUpdate::DownloadFailed(error) => {
                    stats.downloading = stats.downloading.saturating_sub(1);
                    stats.failed += 1;
                    stats.total_processed += 1;
                    stats.errors.push(error);
                    // Keep only last 1000 errors
                    if stats.errors.len() > 1000 {
                        stats.errors.remove(0);
                    }
                }
                StatsUpdate::ShardChanged(shard) => {
                    stats.current_shard = shard;
                }
                StatsUpdate::MemoryUsage(mb) => {
                    stats.memory_usage_mb = mb;
                }
            }

            // Update download rate every second
            let now = Instant::now();
            if now.duration_since(last_update_time) >= Duration::from_secs(1) {
                let success_diff = stats.success.saturating_sub(last_success_count);
                let time_diff = now.duration_since(last_update_time).as_secs_f64();
                stats.download_rate = success_diff as f64 / time_diff;

                last_success_count = stats.success;
                last_update_time = now;
            }
        }
    });

    (stats, tx)
}
