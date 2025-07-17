use std::time::Duration;

use anyhow::Result;
use opentelemetry::{self, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::WithExportConfig;
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

fn init_logger_provider(log_level: tracing::Level, enable_otel: bool, otel_endpoint: String) {
    if enable_otel {
        let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
            .with_resource(get_resource())
            .with_batch_exporter(
                opentelemetry_otlp::LogExporter::builder()
                    .with_tonic()
                    .with_endpoint(otel_endpoint)
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
    } else {
        let filter_fmt = EnvFilter::new(log_level.to_string());
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_thread_names(true)
            .with_filter(filter_fmt);

        tracing_subscriber::registry().with(fmt_layer).init();
    }
}

// 只在 enable_otel 时初始化 tracer/meter provider
fn init_tracer_provider(enable_otel: bool, otel_endpoint: String) {
    if !enable_otel {
        return;
    }

    global::set_text_map_propagator(TraceContextPropagator::new());

    let builder =
        opentelemetry_sdk::trace::SdkTracerProvider::builder().with_resource(get_resource());
    let provider = builder
        .with_batch_exporter(
            SpanExporter::builder()
                .with_tonic()
                .with_endpoint(otel_endpoint)
                .build()
                .expect("Failed to initialize tracing provider"),
        )
        .build();

    global::set_tracer_provider(provider);
}

fn init_meter_provider(enable_otel: bool, otel_endpoint: String) -> SdkMeterProvider {
    if !enable_otel {
        return SdkMeterProvider::default();
    }

    let builder =
        opentelemetry_sdk::metrics::SdkMeterProvider::builder().with_resource(get_resource());

    let provider = builder
        .with_periodic_exporter(
            opentelemetry_otlp::MetricExporter::builder()
                .with_temporality(opentelemetry_sdk::metrics::Temporality::Delta)
                .with_tonic()
                .with_endpoint(otel_endpoint)
                .build()
                .expect("Failed to initialize metric exporter"),
        )
        .build();

    global::set_meter_provider(provider.clone());
    provider
}

#[derive(Debug, Clone)]
pub struct ObservabilityOptions {
    pub log_level: tracing::Level,
    pub enable_otel: bool,
    pub otel_endpoint: String,
}

pub struct ObservabilityManager {
    options: ObservabilityOptions,
    meter_provider: Option<SdkMeterProvider>,
}

impl ObservabilityManager {
    pub fn new(options: ObservabilityOptions) -> Self {
        init_logger_provider(
            options.log_level,
            options.enable_otel,
            options.otel_endpoint.clone(),
        );
        if options.enable_otel {
            init_tracer_provider(options.enable_otel, options.otel_endpoint.clone());
        }
        let meter_provider = match options.enable_otel {
            true => Some(init_meter_provider(
                options.enable_otel,
                options.otel_endpoint.clone(),
            )),
            false => None,
        };

        Self {
            options,
            meter_provider,
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(meter_provider) = self.meter_provider.take() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            meter_provider.shutdown()?;
        }
        Ok(())
    }
}
