use anyhow::Result;
use axum::{Router, routing::get};
use lazy_static::lazy_static;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::fs::OpenOptions;
use std::net::SocketAddr;
use tracing_subscriber::{
    Registry, fmt::writer::MakeWriterExt, layer::SubscriberExt, util::SubscriberInitExt,
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
    install_observability_with_log_redirect(false)
}

pub fn install_observability_with_log_redirect(redirect_to_file: bool) -> Result<()> {
    setup_metrics_recorder();

    if redirect_to_file {
        // When TUI is enabled, redirect logs to a file
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("img2dataset-rs.log")?;

        Registry::default()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(log_file.with_max_level(tracing::Level::INFO))
                    .with_ansi(false), // No ANSI colors in log file
            )
            .init();

        // Also setup a separate writer for errors to stderr when TUI is running
        // but only for critical errors
        eprintln!("Logs are being written to img2dataset-rs.log");
    } else {
        // Normal console logging when TUI is not enabled
        Registry::default()
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    start_metrics_server()?;

    Ok(())
}
