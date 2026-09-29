#!/usr/bin/env python3
"""
Hyperlocal Food-Delivery Event Simulator -- entrypoint.

Examples:
    # dry run, no Kafka needed, prints events to stdout
    python main.py --output-mode console --duration 5 --speed-factor 120

    # write newline-delimited JSON files instead (one per topic)
    python main.py --output-mode file --duration 30

    # real run against a local Kafka broker (see README for docker-compose)
    python main.py --output-mode kafka --bootstrap-servers localhost:9092
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from simulator.config import AppConfig
from simulator.engine import SimulatorEngine
from simulator.logging_config import setup_logging

log = logging.getLogger("main")


def _load_dotenv_if_present() -> None:
    """Optional: auto-load a .env file (Confluent Cloud credentials, etc.)
    if python-dotenv is installed. Never required -- exporting the same
    variables in your shell works identically."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    if load_dotenv():
        log.info("Loaded environment variables from .env")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Hyperlocal food-delivery event simulator")
    p.add_argument("--config", default="config/config.yaml", help="Path to config YAML")
    p.add_argument("--output-mode", choices=["kafka", "console", "file"], default=None,
                   help="Override output.mode from config")
    p.add_argument("--bootstrap-servers", default=None, help="Override kafka.bootstrap_servers")
    p.add_argument("--duration", type=float, default=None, help="Override simulation.duration_minutes")
    p.add_argument("--speed-factor", type=float, default=None, help="Override simulation.speed_factor")
    p.add_argument("--seed", type=int, default=None, help="Override simulation.random_seed")
    p.add_argument("--num-customers", type=int, default=None, help="Override scale.num_customers")
    p.add_argument("--num-restaurants", type=int, default=None, help="Override scale.num_restaurants")
    p.add_argument("--num-dps", type=int, default=None, help="Override scale.num_delivery_partners")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def apply_overrides(cfg: AppConfig, args: argparse.Namespace) -> AppConfig:
    if args.output_mode:
        cfg.output.mode = args.output_mode
    if args.bootstrap_servers:
        cfg.kafka.bootstrap_servers = args.bootstrap_servers
    if args.duration is not None:
        cfg.simulation.duration_minutes = args.duration
    if args.speed_factor is not None:
        cfg.simulation.speed_factor = args.speed_factor
    if args.seed is not None:
        cfg.simulation.random_seed = args.seed
    if args.num_customers is not None:
        cfg.scale.num_customers = args.num_customers
    if args.num_restaurants is not None:
        cfg.scale.num_restaurants = args.num_restaurants
    if args.num_dps is not None:
        cfg.scale.num_delivery_partners = args.num_dps
    return cfg


async def main_async() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    _load_dotenv_if_present()

    cfg = AppConfig.load(args.config)
    cfg = apply_overrides(cfg, args)

    log.info("Starting simulator | output.mode=%s | duration=%.1fmin | speed_factor=%s",
              cfg.output.mode, cfg.simulation.duration_minutes, cfg.simulation.speed_factor)

    engine = SimulatorEngine(cfg)
    try:
        await engine.run()
    except asyncio.CancelledError:
        pass
    log.info("Simulator stopped.")


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("Interrupted by user, shutting down.")


if __name__ == "__main__":
    main()
