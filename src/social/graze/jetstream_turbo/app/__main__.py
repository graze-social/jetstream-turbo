from logging.config import dictConfig
import argparse
import asyncio
import json
import logging
import os

from social.graze.jetstream_turbo.app.turbocharger import start_turbo_charger
from social.graze.jetstream_turbo.app.config import Settings
from social.graze.jetstream_turbo.app.metrics import start_metrics_server


def configure_logging():
    """
    Configures logging.
    """
    logging_config_file = os.getenv("LOGGING_CONFIG_FILE", "")

    if len(logging_config_file) > 0:
        with open(logging_config_file, encoding="utf-8") as fl:
            dictConfig(json.load(fl))
        return

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def start():
    """
    Parse arguments and start turbocharger.
    """

    configure_logging()

    parser = argparse.ArgumentParser(prog="turbocharger")
    parser.add_argument(
        "--modulo",
        type=int,
        default=0,
        help="Modulo to test against",
    )
    parser.add_argument(
        "--shard",
        type=int,
        default=0,
        help="Shard to test against",
    )
    args = parser.parse_args()

    # Start Prometheus metrics server
    settings = Settings()
    start_metrics_server(settings.metrics_port)

    await start_turbo_charger(
        settings,
        modulo=args.modulo,
        shard=args.shard,
    )

def main():
    """
    The entrypoint for the program, called if this module is invoked and also in `pyproject.toml`.
    """
    asyncio.run(start())

if __name__ == "__main__":
    main()

