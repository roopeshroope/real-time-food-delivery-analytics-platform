import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # avoid duplicate handlers if setup_logging is called more than once
    if root.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03dZ %(levelname)-7s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(fmt)
    root.addHandler(handler)

    # kafka-python is noisy at INFO; keep it at WARNING unless debugging
    logging.getLogger("kafka").setLevel(logging.WARNING)
