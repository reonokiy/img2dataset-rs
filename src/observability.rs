use std::time::Duration;

use anyhow::Result;
use opentelemetry::{self, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_resource_detectors::OsResourceDetector;
use opentelemetry_resource_detectors::ProcessResourceDetector;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::resource::ResourceDetector;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn get_resource() -> Resource {
    let detectors: Vec<Box<dyn ResourceDetector>> = vec![
        Box::new(OsResourceDetector),
        Box::new(ProcessResourceDetector),
    ];

    Resource::builder()
        .with_detectors(&detectors)
        .with_service_name("img2dataset-rs")
        .build()
}

fn init_tracer_provider() {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_resource(get_resource())
        .with_batch_exporter(
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .build()
                .expect("Failed to initialize tracing provider"),
        )
        .build();

    global::set_tracer_provider(tracer_provider);
}

fn init_meter_provider() -> SdkMeterProvider {
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(get_resource())
        .with_periodic_exporter(
            opentelemetry_otlp::MetricExporter::builder()
                .with_temporality(opentelemetry_sdk::metrics::Temporality::Delta)
                .with_tonic()
                .build()
                .expect("Failed to initialize metric exporter"),
        )
        .with_periodic_exporter(opentelemetry_stdout::MetricExporter::default())
        .build();
    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

fn init_logger_provider(log_level: tracing::Level) {
    let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_resource(get_resource())
        .with_batch_exporter(
            opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .build()
                .expect("Failed to initialize logger provider"),
        )
        .build();
    let filter_otel = EnvFilter::new(log_level.to_string());
    let otel_layer = OpenTelemetryTracingBridge::new(&logger_provider).with_filter(filter_otel);
    let filter_fmt = EnvFilter::new(log_level.to_string());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_filter(filter_fmt);

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer)
        .init();
}

#[derive(Debug, Clone)]
pub struct ObservabilityOptions {
    pub log_level: tracing::Level,
}

pub struct ObservabilityManager {
    options: ObservabilityOptions,
    meter_provider: SdkMeterProvider,
}

impl ObservabilityManager {
    pub fn new(options: ObservabilityOptions) -> Self {
        init_logger_provider(options.log_level);
        init_tracer_provider();
        let meter_provider = init_meter_provider();

        Self {
            options,
            meter_provider,
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.meter_provider.shutdown()?;
        Ok(())
    }
}
