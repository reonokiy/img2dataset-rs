use anyhow::Result;
use axum::{Router, routing::get};
use lazy_static::lazy_static;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::fs::OpenOptions;
use std::net::SocketAddr;
use tracing_subscriber::{
    Registry, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

lazy_static! {
    static ref METRICS_RECORDER_HANDLE: PrometheusHandle =
        PrometheusBuilder::new().install_recorder().unwrap();
}

fn setup_metrics_recorder() {
    let _ = &*METRICS_RECORDER_HANDLE; // Initialize lazy static
}

async fn metrics_handler() -> String {
    METRICS_RECORDER_HANDLE.render()
}

pub fn start_metrics_server() -> Result<()> {
    tokio::spawn(async {
        let app = Router::new().route("/metrics", get(metrics_handler));
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });
    Ok(())
}

pub fn install_observability() -> Result<()> {
    install_observability_with_config(false, tracing::Level::INFO)
}

pub fn install_observability_with_log_redirect(redirect_to_file: bool) -> Result<()> {
    install_observability_with_config(redirect_to_file, tracing::Level::INFO)
}

pub fn install_observability_with_config(
    redirect_to_file: bool,
    log_level: tracing::Level,
) -> Result<()> {
    setup_metrics_recorder();

    let level_filter = LevelFilter::from_level(log_level);

    if redirect_to_file {
        // When status line is enabled, redirect logs to a file
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("img2dataset-rs.log")?;

        Registry::default()
            .with(level_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(log_file)
                    .with_ansi(false), // No ANSI colors in log file
            )
            .init();

        // Also setup a separate writer for errors to stderr when status line is running
        // but only for critical errors
        eprintln!("Logs are being written to img2dataset-rs.log");
    } else {
        // Normal console logging when status line is not enabled
        Registry::default()
            .with(level_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    start_metrics_server()?;

    Ok(())
}
