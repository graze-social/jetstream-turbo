"""
Prometheus metrics for jetstream-turbo.
"""
import logging
from prometheus_client import Counter, Histogram, start_http_server

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
