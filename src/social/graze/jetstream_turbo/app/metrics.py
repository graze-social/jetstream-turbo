"""
Prometheus metrics for jetstream-turbo.
"""
import logging
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

# Metrics
posts_processed = Counter(
    'jetstream_turbo_posts_processed_total',
    'Total number of posts processed by the turbocharger'
)

batch_processing_time = Histogram(
    'jetstream_turbo_batch_processing_seconds',
    'Time taken to process a batch of posts',
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, float('inf'))
)

# Why the jetstream connection was re-established. See `client.reconnect_reason`.
RECONNECT_REASONS = (
    "handshake", "closed_ok", "closed_error", "idle", "timeout", "os_error", "other",
)
reconnects = Counter(
    'jetstream_turbo_reconnects_total',
    'Jetstream reconnects, by the reason the previous connection ended',
    ['reason'],
)

hydration_retries = Counter(
    'jetstream_turbo_hydration_retries_total',
    'Batches whose hydration failed and was retried'
)

unhydrated_records = Counter(
    'jetstream_turbo_unhydrated_records_total',
    'Records egressed without hydration because every hydration attempt failed'
)

# `egress` is the method's `name`; `reason` is "exhausted" (retries ran out, the batch is held
# back from the cursor) or "rejected" (SQS refused the message itself, so a retry cannot help).
EGRESS_NAMES = ("s3", "sqs", "redis")
EGRESS_FAILURE_REASONS = ("exhausted", "rejected")
egress_retries = Counter(
    'jetstream_turbo_egress_retries_total',
    'Egress attempts that failed and were retried',
    ['egress'],
)
egress_failures = Counter(
    'jetstream_turbo_egress_failures_total',
    'Records that could not be egressed',
    ['egress', 'reason'],
)

cursor_lag_seconds = Gauge(
    'jetstream_turbo_cursor_lag_seconds',
    'Seconds between now and the persisted jetstream cursor (what a restart would replay)'
)

# A labelled series does not exist until it is first incremented, so an alert on it cannot
# fire until the very fault it is watching for has already happened once. Create every
# combination at zero up front.
for _reason in RECONNECT_REASONS:
    reconnects.labels(reason=_reason)
for _name in EGRESS_NAMES:
    egress_retries.labels(egress=_name)
    for _reason in EGRESS_FAILURE_REASONS:
        egress_failures.labels(egress=_name, reason=_reason)


def start_metrics_server(port: int = 8000):
    """
    Start the Prometheus metrics HTTP server.

    Args:
        port: Port to expose metrics on (default: 8000)
    """
    try:
        start_http_server(port)
        logger.info("Prometheus metrics server started on port %d", port)
    except Exception as e:
        logger.error("Failed to start metrics server: %s", e)
        raise
