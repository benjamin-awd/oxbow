use axum::{Router, routing::get};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::log::*;

/// Tracks the health of the main processing loop via heartbeat timestamps.
#[derive(Clone)]
pub struct Heartbeat {
    last_update: Arc<AtomicU64>,
    max_age_secs: u64,
}

impl Heartbeat {
    /// `max_age_secs` is the maximum time since the last heartbeat before
    /// the service is considered unhealthy.
    pub fn new(max_age_secs: u64) -> Self {
        Self {
            last_update: Arc::new(AtomicU64::new(current_timestamp())),
            max_age_secs,
        }
    }

    pub fn touch(&self) {
        self.last_update
            .store(current_timestamp(), Ordering::Relaxed);
    }

    /// Returns `Ok(())` if healthy, or `Err(age_secs)` if stale.
    pub fn check(&self) -> Result<(), u64> {
        let last = self.last_update.load(Ordering::Relaxed);
        let now = current_timestamp();
        let age = now.saturating_sub(last);

        if age > self.max_age_secs {
            Err(age)
        } else {
            Ok(())
        }
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Exposes a `/health` endpoint that returns 200 OK if the heartbeat is fresh,
/// or 503 Service Unavailable if the main loop appears to be stale/stuck.
pub fn spawn_health_server(addr: String, heartbeat: Heartbeat) {
    tokio::spawn(async move {
        let app = Router::new().route(
            "/health",
            get(move || {
                let hb = heartbeat.clone();
                async move {
                    match hb.check() {
                        Ok(()) => (axum::http::StatusCode::OK, "ok".to_string()),
                        Err(age) => (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            format!("unhealthy: main loop stale ({}s since last heartbeat)", age),
                        ),
                    }
                }
            }),
        );

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind health server to {addr}: {e}");
                return;
            }
        };

        info!("Health server listening on {addr}");
        if let Err(e) = axum::serve(listener, app).await {
            error!("Health server error: {e}");
        }
    });
}
