"""
CoinGecko API Operator — fetches Bitcoin historical OHLC + market data.

Fetches market chart data from CoinGecko using the official coingecko-sdk
client and stores raw JSON in GCS.
Loading into BigQuery is handled by GCSToBigQueryOperator in the DAG.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from operators.base_http_operator import BaseIngestionOperator

log = logging.getLogger(__name__)

COIN_CATALOG = {
    "bitcoin": {"symbol": "BTC", "name": "Bitcoin"},
}


class CoinGeckoOperator(BaseIngestionOperator):
    """
    Fetches Bitcoin (and optionally other coin) daily OHLC data from CoinGecko
    and saves raw JSON to GCS.

    Uses the official ``coingecko-sdk`` Python client.

    Args:
        coin_ids: List of CoinGecko coin IDs (default: ['bitcoin']).
        vs_currency: Quote currency (default 'usd').
        api_key: CoinGecko demo API key (optional).
    """

    ui_color = "#F7B731"

    source_name = "coingecko"
    rate_limit_delay = 1.5
    retry_base_delay = 10.0
    retry_backoff_factor = 3.0

    def __init__(
        self,
        *,
        coin_ids: list[str] | None = None,
        vs_currency: str = "usd",
        api_key: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.coin_ids = coin_ids or ["bitcoin"]
        self.vs_currency = vs_currency
        self.api_key = api_key
        self._client = None

    # ── Abstract interface ────────────────────────────────────────────────────

    def get_assets(self) -> list[str]:
        return self.coin_ids

    def fetch_asset(self, coin_id: str, start: date, end: date) -> Any:
        client = self._get_client()
        log.info("Fetching %s via CoinGecko client (%s → %s)", coin_id, start, end)

        response = client.coins.market_chart.get_range(
            id=coin_id,
            vs_currency=self.vs_currency,
            from_=start.isoformat(),
            to=end.isoformat(),
        )
        return {
            "coin_id": coin_id,
            "vs_currency": self.vs_currency,
            "fetch_start": start.isoformat(),
            "fetch_end": end.isoformat(),
            "raw_json": response.to_json(),
        }

    # ── Retry classification ───────────────────────────────────────────────────

    def is_retryable(self, exc: Exception) -> bool:
        from coingecko_sdk import (
            APIConnectionError,
            AuthenticationError,
            BadRequestError,
            InternalServerError,
            NotFoundError,
            PermissionDeniedError,
            RateLimitError,
            UnprocessableEntityError,
        )

        if isinstance(exc, (BadRequestError, PermissionDeniedError, NotFoundError,
                            UnprocessableEntityError, AuthenticationError)):
            return False
        if isinstance(exc, (RateLimitError, InternalServerError, APIConnectionError)):
            return True
        return True

    # ── Client ─────────────────────────────────────────────────────────────────

    def _get_client(self):
        if self._client is None:
            from coingecko_sdk import Coingecko

            self._client = Coingecko(demo_api_key=self.api_key, environment="demo")
        return self._client
