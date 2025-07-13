use indicatif::{ProgressBar, ProgressStyle};
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
            start_time: Instant::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StatsUpdate {
    DownloadStarted,
    DownloadSuccess,
    DownloadFailed(String),
    ShardChanged(u64),
    MemoryUsage(f64),
    TotalProcessed(u64),
}

pub struct StatusLine {
    progress_bar: ProgressBar,
    stats: Arc<RwLock<DownloadStats>>,
}

impl StatusLine {
    pub fn new(stats: Arc<RwLock<DownloadStats>>) -> Self {
        // Create a progress bar that acts as a status line
        let progress_bar = ProgressBar::new_spinner();
        progress_bar.set_style(
            ProgressStyle::with_template(
                "📊 {spinner} {msg}"
            )
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
        );
        
        Self { 
            progress_bar,
            stats,
        }
    }

    pub async fn update_display(&self) {
        let stats = self.stats.read().await;
        let total = stats.success + stats.failed;
        let success_rate = if total > 0 {
            stats.success as f64 / total as f64 * 100.0
        } else {
            0.0
        };

        let elapsed = stats.start_time.elapsed();
        let elapsed_str = format!(
            "{}:{:02}:{:02}",
            elapsed.as_secs() / 3600,
            (elapsed.as_secs() % 3600) / 60,
            elapsed.as_secs() % 60
        );

        // Create comprehensive status message
        let message = format!(
            "Downloaded: {} | Success: {} ({:.1}%) | Failed: {} | In Progress: {} | Rate: {:.1}/s | Memory: {:.1}MB | Runtime: {} | Shard: {}",
            total,
            stats.success,
            success_rate,
            stats.failed,
            stats.downloading,
            stats.download_rate,
            stats.memory_usage_mb,
            elapsed_str,
            stats.current_shard
        );

        self.progress_bar.set_message(message);
    }

    pub async fn run_status_display(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            self.update_display().await;
            self.progress_bar.tick();
        }
    }
}

impl Drop for StatusLine {
    fn drop(&mut self) {
        self.progress_bar.finish_and_clear();
    }
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
                StatsUpdate::DownloadFailed(_) => {
                    stats.downloading = stats.downloading.saturating_sub(1);
                    stats.failed += 1;
                    stats.total_processed += 1;
                }
                StatsUpdate::ShardChanged(shard) => {
                    stats.current_shard = shard;
                }
                StatsUpdate::MemoryUsage(mb) => {
                    stats.memory_usage_mb = mb;
                }
                StatsUpdate::TotalProcessed(total) => {
                    stats.total_processed = total;
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

pub async fn run_status_line(stats: Arc<RwLock<DownloadStats>>) {
    let status_line = StatusLine::new(stats);
    status_line.run_status_display().await;
}

// Cleanup function - indicatif handles this automatically with Drop
pub fn cleanup_status_line() {
    // indicatif handles cleanup automatically when StatusLine is dropped
}
