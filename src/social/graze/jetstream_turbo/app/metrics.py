"""
Prometheus metrics for jetstream-turbo.
"""
import logging
from prometheus_client import Gauge, Histogram, start_http_server, REGISTRY, CollectorRegistry
from prometheus_client.core import GaugeMetricFamily

logger = logging.getLogger(__name__)


class PostsProcessedCollector:
    """Custom collector that provides posts processed since last scrape."""

    def __init__(self):
        self._count = 0

    def increment(self, amount: int):
        """Increment the counter by the given amount."""
        self._count += amount

    def collect(self):
        """Called by Prometheus when scraping. Returns and resets the counter."""
        metric = GaugeMetricFamily(
            'jetstream_turbo_posts_processed_since_last_scrape',
            'Number of posts processed since the last Prometheus scrape'
        )
        metric.add_metric([], self._count)
        self._count = 0  # Reset after reporting
        yield metric


# Create the custom collector instance
posts_processed_collector = PostsProcessedCollector()

# Metrics
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
        # Register the custom collector
        REGISTRY.register(posts_processed_collector)
        start_http_server(port)
        logger.info("Prometheus metrics server started on port %d", port)
    except Exception as e:
        logger.error("Failed to start metrics server: %s", e)
        raise
