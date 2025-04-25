import os
from aiohttp import web
import logging
from logging.config import dictConfig
import argparse
import json


def configure_logging():
    logging_config_file = os.getenv("LOGGING_CONFIG_FILE", "")

    if len(logging_config_file) > 0:
        with open(logging_config_file) as fl:
            dictConfig(json.load(fl))
        return

    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)


def main():
    configure_logging()

    from social.graze.jetstream_turbo.app.turbocharger import start_turbo_charger

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
    web.run_app(
        start_turbo_charger(
            None,
            modulo=args.modulo,
            shard=args.shard,
        )
    )


if __name__ == "__main__":
    main()
