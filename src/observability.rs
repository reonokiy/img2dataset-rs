use anyhow::Result;
use axum::{Router, routing::get};
use lazy_static::lazy_static;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use tracing_subscriber::{Registry, layer::SubscriberExt, util::SubscriberInitExt};

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
    setup_metrics_recorder();

    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .init();

    start_metrics_server()?;

    Ok(())
}
