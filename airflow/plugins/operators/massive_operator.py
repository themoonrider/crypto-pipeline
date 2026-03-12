"""
Massive API Operator — fetches stocks, forex, and index data.

Fetches OHLCV data from the Massive API using the
official Python client and stores raw JSON in GCS.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from operators.base_http_operator import BaseIngestionOperator

log = logging.getLogger(__name__)

class MassiveAPIOperator(BaseIngestionOperator):
    """
    Fetches OHLCV data from Massive API for configured symbols and saves to GCS.

    Uses the official ``massive`` Python client (``from massive import RESTClient``).

    Args:
        symbols: List of asset keys, followed official ticker symbol at https://deepwiki.com/massive-com/client-python/3.3.1-tickers-and-ticker-details#listing-tickers .
        massive_api_key: API key for Massive.
    """

    ui_color = "#4A90D9"

    source_name = "massive"
    rate_limit_delay = 0.5
    retry_base_delay = 5.0
    retry_backoff_factor = 2.0

    def __init__(
        self,
        *,
        symbols: list[str] | None = None,
        massive_api_key: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.symbols = symbols
        self.massive_api_key = massive_api_key
        self._client = None

    # ── Abstract interface ────────────────────────────────────────────────────

    def get_assets(self) -> list[str]:
        return self.symbols

    def fetch_asset(self, symbol: str, start: date, end: date) -> Any:

        client = self._get_client()

        log.info("Fetching %s via Massive client (%s → %s)", symbol, start, end)

        aggs = list(client.list_aggs(
            ticker=symbol,
            multiplier=1,
            timespan="day",
            from_=start.isoformat(),
            to=end.isoformat()
        ))

        return {
            "symbol": symbol,
            "fetch_start": start.isoformat(),
            "fetch_end": end.isoformat(),
            "results": [
                {
                    "timestamp": agg.timestamp,
                    "open": agg.open,
                    "high": agg.high,
                    "low": agg.low,
                    "close": agg.close,
                    "volume": agg.volume,
                }
                for agg in aggs
            ],
        }

    # ── Retry classification ───────────────────────────────────────────────────

    def is_retryable(self, exc: Exception) -> bool:
        from massive.exceptions import AuthError, BadResponse

        if isinstance(exc, AuthError):
            return False
        if isinstance(exc, BadResponse):
            msg = str(exc)
            return any(code in msg for code in ("429", "500", "502", "503", "504"))
        return True

    # ── Client ─────────────────────────────────────────────────────────────────

    def _get_client(self):
        if self._client is None:
            from massive import RESTClient

            self._client = RESTClient(api_key=self.massive_api_key)
        return self._client
