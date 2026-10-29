from social.graze.jetstream_turbo.app.config import Settings
from social.graze.jetstream_turbo.app.egress import RedisEgress
from social.graze.jetstream_turbo.app.turbocharger import get_egress_methods


def _redis_egress(settings):
    return next(e for e in get_egress_methods(settings) if isinstance(e, RedisEgress))


def test_redis_stream_is_trimmed_generously_by_default():
    settings = Settings(output_to_redis=True, redis_url="redis://localhost", stream_name="s")
    assert _redis_egress(settings).trim_maxlen == 20000


def test_redis_trim_is_env_overridable_and_zero_disables(monkeypatch):
    monkeypatch.setenv("REDIS_TRIM_MAXLEN", "500")
    assert _redis_egress(Settings(output_to_redis=True, redis_url="redis://x", stream_name="s")).trim_maxlen == 500
    monkeypatch.setenv("REDIS_TRIM_MAXLEN", "0")
    assert _redis_egress(Settings(output_to_redis=True, redis_url="redis://x", stream_name="s")).trim_maxlen is None
