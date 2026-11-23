"""Lookback configuration for ingestion and the Docker runtime."""

import os
import re
from collections.abc import Mapping


LOOKBACK_KEY = "INGESTION_LOOKBACK_SECONDS"


def read_lookback_seconds(env: Mapping[str, str] | None = None) -> int:
    if env is None:
        env = os.environ
    raw = env.get(LOOKBACK_KEY, "7200").strip()
    if not re.fullmatch(r"[0-9]{1,10}", raw) or int(raw) > 2_147_483_647:
        raise ValueError(f"{LOOKBACK_KEY} must be an integer from 0 to 2147483647")
    return int(raw)
