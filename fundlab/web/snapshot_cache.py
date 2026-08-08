"""Non-blocking, single-flight snapshot metadata used by the local dashboard."""

from __future__ import annotations

from dataclasses import dataclass
import gc
import json
from pathlib import Path
from threading import Lock, Thread
import time
from typing import Any, Iterable, Mapping

from fundlab.marketdata import MarketDataWarehouse


@dataclass(frozen=True)
class _PointerToken:
    snapshot_id: str
    manifest_sha256: str


@dataclass(frozen=True)
class _RefreshResult:
    token: _PointerToken
    manifest: Any
    summary: Mapping[str, Any]
    names: Mapping[str, str]
    resolved_ids: frozenset[str]


class InstrumentNameResolver:
    """Serve last-good display names while canonical refreshes off request threads."""

    def __init__(
        self,
        market_data_root: str | Path,
        *,
        warehouse: Any | None = None,
        refresh_delay_seconds: float = 0.5,
    ) -> None:
        if refresh_delay_seconds < 0:
            raise ValueError("refresh_delay_seconds must not be negative")
        self.root = Path(market_data_root).resolve()
        self._warehouse = warehouse or MarketDataWarehouse(self.root)
        self._refresh_delay_seconds = refresh_delay_seconds
        self._lock = Lock()
        self._observed_token: _PointerToken | None = None
        self._bound_token: _PointerToken | None = None
        self._bound_manifest: Any | None = None
        self._known_ids: set[str] = set()
        self._resolved_ids: set[str] = set()
        self._pending_ids: set[str] = set()
        self._names: dict[str, str] = {}
        self._summary: dict[str, Any] | None = None
        self._refreshing = False
        self._failures = 0
        self._retry_at = 0.0
        self._last_error: str | None = None
        self._closed = False

    def lookup(self, instrument_ids: Iterable[str]) -> dict[str, str]:
        names, _ = self.lookup_state(instrument_ids)
        return names

    def lookup_state(
        self, instrument_ids: Iterable[str],
    ) -> tuple[dict[str, str], bool]:
        """Return names and pending status from one consistent cache view."""
        requested = {
            str(item).strip() for item in instrument_ids if str(item).strip()
        }
        self._observe(requested)
        with self._lock:
            names = {
                instrument_id: self._names[instrument_id]
                for instrument_id in requested
                if instrument_id in self._names
            }
            pending = any(item not in self._resolved_ids for item in requested)
            return names, pending

    def pending(self, instrument_ids: Iterable[str]) -> bool:
        """Whether requested IDs are still waiting for canonical resolution."""
        _, pending = self.lookup_state(instrument_ids)
        return pending

    def market_summary(self) -> dict[str, Any]:
        self._observe(())
        with self._lock:
            if self._summary is not None:
                return dict(self._summary)
            detail = self._last_error or "canonical snapshot metadata is refreshing"
            return {"error": detail}

    def prewarm(self, instrument_ids: Iterable[str] = ()) -> None:
        self._observe(instrument_ids)

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def _observe(self, requested: Iterable[str]) -> None:
        try:
            token = _read_pointer_token(self.root)
        except Exception as exc:
            with self._lock:
                self._last_error = f"{type(exc).__name__}: {exc}"
            return
        now = time.monotonic()
        with self._lock:
            if self._closed:
                return
            requested_ids = set(requested)
            self._known_ids.update(requested_ids)
            if token != self._observed_token:
                self._observed_token = token
                self._resolved_ids.clear()
                self._pending_ids.update(self._known_ids)
                self._failures = 0
                self._retry_at = 0.0
            else:
                self._pending_ids.update(requested_ids - self._resolved_ids)
            if now >= self._retry_at:
                self._schedule_locked()

    def _schedule_locked(self) -> None:
        if self._closed or self._observed_token is None:
            return
        if self._refreshing:
            return
        needs_binding = self._bound_token != self._observed_token
        if not needs_binding and not self._pending_ids:
            return
        token = self._observed_token
        requested_ids = frozenset(
            self._known_ids if needs_binding else self._pending_ids
        )
        self._pending_ids.difference_update(requested_ids)
        manifest = self._bound_manifest if self._bound_token == token else None
        self._refreshing = True
        try:
            Thread(
                target=self._refresh_and_commit,
                args=(token, requested_ids, manifest),
                name="fundlab-web-snapshot",
                daemon=True,
            ).start()
        except Exception:
            self._refreshing = False
            self._pending_ids.update(requested_ids)
            raise

    def _refresh(
        self,
        token: _PointerToken,
        requested_ids: frozenset[str],
        manifest: Any | None,
    ) -> _RefreshResult:
        if manifest is None:
            manifest = self._warehouse.load_current_snapshot()
        if manifest.snapshot_id != token.snapshot_id:
            raise RuntimeError("canonical snapshot changed during dashboard refresh")
        scope = manifest.plan.universe_scope
        summary = {
            "snapshot_id": manifest.snapshot_id,
            "published_end": None if scope is None else scope.history_end.isoformat(),
            "history_start": None if scope is None else scope.history_start.isoformat(),
            "instruments": 0 if scope is None else len(scope.instrument_ids),
        }
        names: dict[str, str] = {}
        if requested_ids:
            frame = self._warehouse.query_loaded_instrument_names(
                manifest,
                instrument_ids=requested_ids,
            )
            names = {
                str(row.instrument_id): str(row.name)
                for row in frame[["instrument_id", "name"]].itertuples(index=False)
                if row.name is not None and str(row.name).strip()
            }
        return _RefreshResult(token, manifest, summary, names, requested_ids)

    def _refresh_and_commit(
        self,
        token: _PointerToken,
        requested_ids: frozenset[str],
        manifest: Any | None,
    ) -> None:
        if self._refresh_delay_seconds:
            time.sleep(self._refresh_delay_seconds)
        with self._lock:
            if self._closed:
                self._refreshing = False
                return
        try:
            result = self._refresh(token, requested_ids, manifest)
        except Exception as exc:
            result = None
            error = f"{type(exc).__name__}: {exc}"
        else:
            error = None
        finally:
            _release_query_memory()
        with self._lock:
            self._refreshing = False
            if result is not None and result.token == self._observed_token:
                self._bound_token = result.token
                self._bound_manifest = result.manifest
                self._summary = dict(result.summary)
                for instrument_id in result.resolved_ids - result.names.keys():
                    self._names.pop(instrument_id, None)
                self._names.update(result.names)
                self._resolved_ids.update(result.resolved_ids)
                self._pending_ids.difference_update(result.resolved_ids)
                self._failures = 0
                self._retry_at = 0.0
                self._last_error = None
            elif error is not None:
                self._pending_ids.update(self._known_ids - self._resolved_ids)
                self._failures += 1
                delay = min(30.0, 2.0 ** min(self._failures, 5))
                self._retry_at = time.monotonic() + delay
                self._last_error = error
            if self._observed_token != self._bound_token or self._pending_ids:
                if time.monotonic() >= self._retry_at:
                    self._schedule_locked()


def _read_pointer_token(root: Path) -> _PointerToken:
    payload = json.loads((root / "current.json").read_text(encoding="utf-8"))
    snapshot_id = str(payload.get("snapshot_id") or "")
    manifest_sha256 = str(payload.get("manifest_sha256") or "")
    if not snapshot_id.startswith("snap-") or len(manifest_sha256) != 64:
        raise ValueError("invalid canonical current pointer")
    return _PointerToken(snapshot_id, manifest_sha256)


def _release_query_memory() -> None:
    """Return temporary Arrow buffers after the background projection completes."""

    gc.collect()
    try:
        import pyarrow as pa

        pa.default_memory_pool().release_unused()
    except (ImportError, AttributeError):
        pass
