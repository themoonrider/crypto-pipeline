"""
Base Ingestion Operator — shared infrastructure for API-based data ingestion.

Provides date resolution and GCS storage.
Subclasses implement source-specific API fetching via official client libraries.
"""
from __future__ import annotations

import json
import logging
import time
from abc import abstractmethod
from datetime import date, datetime, timezone
from typing import Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context

log = logging.getLogger(__name__)


class BaseIngestionOperator(BaseOperator):
    """
    Abstract base for operators that fetch market data via official client
    libraries and store raw JSON in Google Cloud Storage.

    Subclasses must implement:
        source_name, rate_limit_delay, get_assets, fetch_asset
    """

    template_fields: Sequence[str] = ("fetch_start", "fetch_end")

    max_retries: int = 3
    retry_base_delay: float = 5.0
    retry_backoff_factor: float = 2.0

    def __init__(
        self,
        *,
        fetch_start: str,
        fetch_end: str,
        gcs_bucket: str,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.fetch_start = fetch_start
        self.fetch_end = fetch_end
        self.gcs_bucket = gcs_bucket
        self.gcp_conn_id = gcp_conn_id

    # ── Abstract interface ────────────────────────────────────────────────────

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Short identifier for the data source, e.g. 'massive', 'coingecko'."""

    @abstractmethod
    def get_assets(self) -> list[str]:
        """Return list of asset identifiers to fetch."""

    @abstractmethod
    def fetch_asset(self, asset: str, start: date, end: date) -> Any:
        """Fetch raw data for a single asset using the official client library."""

    def is_retryable(self, exc: Exception) -> bool:
        """Return True if the exception is transient and worth retrying.

        Subclasses should override to classify source-specific exceptions.
        Default: retry everything (assume transient).
        """
        return True

    # ── Shared helpers ────────────────────────────────────────────────────────

    def _resolve_dates(self) -> tuple[date, date]:
        return (
            datetime.strptime(self.fetch_start, "%Y-%m-%d").date(),
            datetime.strptime(self.fetch_end, "%Y-%m-%d").date(),
        )

    def _get_gcs_hook(self) -> GCSHook:
        return GCSHook(gcp_conn_id=self.gcp_conn_id)

    # ── GCS storage ────────────────────────────────────────────────────────────

    def _save_to_gcs(
        self,
        hook: GCSHook,
        identifier: str,
        start: date,
        end: date,
        payload: Any,
    ) -> str:
        key = (
            f"raw/{self.source_name}/{identifier}/"
            f"{start.isoformat()}_{end.isoformat()}_"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
        )
        data = json.dumps(payload).encode("utf-8")
        hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=key,
            data=data,
            mime_type="application/json",
        )
        return key

    # ── Execute ───────────────────────────────────────────────────────────────

    def execute(self, context: Context) -> dict[str, str]:
        """
        Fetch data for all assets and save to GCS.
        Returns dict of {asset: gcs_key} (pushed to XCom automatically).
        """
        start, end = self._resolve_dates()
        assets = self.get_assets()
        run_id = context["run_id"]
        log.info(
            "%s run=%s  %s → %s  assets=%s",
            self.__class__.__name__, context["run_id"], start, end, assets,
        )

        hook = self._get_gcs_hook()
        gcs_keys: dict[str, str] = {}
        failed: list[str] = []

        for asset in assets:
            last_exc = None
            for attempt in range(1, self.max_retries + 1):
                try:
                    raw_data = self.fetch_asset(asset, start, end)
                    raw_data["__de_processed_at"] = datetime.now(timezone.utc).isoformat()
                    raw_data["__run_id"] = run_id
                    key = self._save_to_gcs(hook, asset, start, end, raw_data)
                    gcs_keys[asset] = key
                    log.info("%s → saved to gs://%s/%s", asset, self.gcs_bucket, key)
                    time.sleep(self.rate_limit_delay)
                    break
                except Exception as exc:
                    last_exc = exc
                    if not self.is_retryable(exc):
                        log.error("Fatal error fetching %s: %s", asset, exc, exc_info=True)
                        failed.append(asset)
                        break
                    delay = self.retry_base_delay * (self.retry_backoff_factor ** (attempt - 1))
                    log.warning(
                        "Retryable error fetching %s (attempt %d/%d), retrying in %.1fs: %s",
                        asset, attempt, self.max_retries, delay, exc,
                    )
                    time.sleep(delay)
            else:
                log.error("All %d retries exhausted for %s: %s", self.max_retries, asset, last_exc, exc_info=True)
                failed.append(asset)

        if failed:
            raise RuntimeError("Failed assets: %s", failed)

        return gcs_keys
