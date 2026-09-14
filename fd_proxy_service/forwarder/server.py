"""Per-worker smart forwarder for fd-proxy-service (the ``proxy-fw`` server).

The forwarder runs on each worker cluster (hostNetwork Job) and serves the
SAME acquire/release contract as the control plane (``control/app.py``). scraw
does **not** set ``HTTP_PROXY`` and blind-tunnel ``CONNECT`` through this;
instead, per fetch it:

  1. ``POST /acquire {source}`` → ``{upstream_url, addr_id, provider}``
  2. injects ``upstream_url`` into its own HTTP client (the existing
     ``use_proxy`` / injection path), so the crawler terminates TLS itself
  3. fetches through the picked upstream (e.g. this cluster's gost)
  4. classifies the response via ``ban_rules`` (in the crawler — only it sees
     the decrypted HTTPS body needed to spot a 403 ban)
  5. ``POST /release {source, addr_id, provider, outcome}`` → forwarder
     records the outcome on the circuit

Why not a blind HTTP forward proxy (the literal wording of task 6.1): HTTPS
sources (eastmoney / yahoo / sec / …) use ``CONNECT`` tunneling, and a blind
tunnel cannot inspect the decrypted response needed to classify a ban. The
acquire/release contract puts *selection* at the forwarder and *classification*
at the crawler — both see what they need (the forwarder-shape decision in
``design.md`` Open Questions). The "smart circuit-aware" behavior the user
chose lives in ``acquire_any``: per-request, circuit+rate-limit-aware upstream
pick with sub-second ban rotation (a ban recorded on ``/release`` is visible to
the next ``/acquire`` immediately).

Hot-path optimization (task 6.2): circuit reads go through a **local cached
snapshot** of ``proxy-redis`` (sub-ms in-memory dict lookup vs a cross-cluster
Redis round-trip per fetch). Writes hit ``proxy-redis`` directly via the
NodePort 30381 in ``PROXY_REDIS_URL`` AND update the snapshot immediately, so
the next request in this process sees the new circuit state without waiting for
a refresh. A background thread repopulates the snapshot from
``circuit.all_circuits()`` (a Redis ``scan_iter``) every ``SNAPSHOT_TTL_SEC``.

Ships-dark: if ``proxy-redis`` is unreachable, the snapshot is empty → every
circuit reads CLOSED → ``acquire_any`` routes through ``gost-own`` (or returns
``None`` → the crawler egresses direct). This matches ``circuit.py``'s
graceful-degradation property; fetches still succeed without rotation.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional, Tuple

from fd_proxy_service import circuit

logger = logging.getLogger(__name__)

# Snapshot refresh cadence. Circuit state changes are rare relative to fetch
# rate, so a 2s TTL catches a ban recorded by a sibling forwarder within ~2s
# while keeping proxy-redis load to one ``scan_iter`` per period. A missing key
# means CLOSED = selectable, so a stale snapshot never blocks a healthy upstream
# for long — and a ban recorded in THIS process is reflected instantly via the
# local-cache update in ``record_outcome`` (no refresh wait).
SNAPSHOT_TTL_SEC = 2.0


class CircuitSnapshot:
    """In-process cached view of every circuit in ``proxy-redis``.

    ``is_selectable`` is an in-memory dict lookup (sub-ms). A background refresh
    thread repopulates the dict from ``circuit.all_circuits()`` every
    ``SNAPSHOT_TTL_SEC``. ``record_outcome`` writes straight to proxy-redis
    (via the original ``circuit.record_outcome``) AND updates the local dict
    immediately so the change is visible to the next request in this process.

    Entries are keyed by ``(source, unit)`` — the same key the Redis circuit
    uses, so addresses sharing an exit IP share one cached entry and a ban on
    that exit is visible for the whole group on the next lookup.

    Only ``is_selectable`` and ``record_outcome`` are patched onto the
    ``circuit`` module — ``get_state`` is left as the original Redis read so the
    write-path state machine (inside ``record_outcome``) computes from live
    Redis state, never from a possibly-stale cache entry. This keeps the
    counter race no worse than the original (which already does a non-atomic
    ``hgetall`` → ``hset``).
    """

    def __init__(self, ttl_sec: float = SNAPSHOT_TTL_SEC) -> None:
        self._ttl = ttl_sec
        self._cache: dict[Tuple[str, str], dict] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Captured at install time, before patching. ``record_outcome`` calls
        # this (not the module attribute) to avoid recursing through the patch.
        self._orig_record_outcome = None  # type: ignore[var-annotated]

    def start(self) -> None:
        """Start the background refresh thread (daemon)."""
        if self._thread is not None:
            return
        # Prime immediately so the first acquire sees current state (otherwise
        # the cache is empty until the first TTL tick — harmless, but a priming
        # read avoids a brief all-CLOSED window at startup).
        try:
            self._refresh()
        except Exception as e:  # noqa: BLE001 - priming failure is non-fatal
            logger.debug("circuit snapshot prime failed: %s", e)
        self._thread = threading.Thread(
            target=self._refresh_loop, daemon=True, name="circuit-snapshot"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._refresh()
            except Exception as e:  # noqa: BLE001 - refresh must never crash the process
                logger.debug("circuit snapshot refresh failed: %s", e)
            self._stop.wait(self._ttl)

    def _refresh(self) -> None:
        rows = circuit.all_circuits()
        fresh: dict[Tuple[str, str], dict] = {}
        for r in rows:
            fresh[(r["source"], r["unit"])] = r
        with self._lock:
            self._cache = fresh

    def get_state(self, source: str, unit: str) -> dict:
        """In-memory lookup; absent key = CLOSED (no circuit state yet).

        This is the read path for ``is_selectable``. The *write* path
        (``record_outcome``) deliberately does NOT call this — it goes through
        the original Redis-backed ``get_state`` inside ``_orig_record_outcome``.
        """
        with self._lock:
            st = self._cache.get((source, unit))
        if st is None:
            return {"state": "closed", "fail_streak": 0, "success_streak": 0,
                    "cooldown_until": None, "open_cycles": 0, "permanent": False}
        return st

    def is_selectable(self, source: str, unit: str) -> bool:
        st = self.get_state(source, unit)
        if st.get("permanent"):
            return False
        return st.get("state", "closed") == "closed"

    def record_outcome(self, source: str, unit: str, outcome: str) -> dict:
        """Write to proxy-redis (source of truth) + update the local snapshot.

        Delegates the Redis write + state-machine computation to the ORIGINAL
        ``circuit.record_outcome`` (captured before patching), which reads live
        Redis state internally — never this cache. Then mirrors the returned
        new state into the local dict so the next ``is_selectable`` in this
        process reflects it without waiting for a refresh tick.
        """
        new_state = self._orig_record_outcome(source, unit, outcome)
        with self._lock:
            self._cache[(source, unit)] = {
                "source": source, "unit": unit,
                "state": new_state.get("state", "closed"),
                "fail_streak": new_state.get("fail_streak", 0),
                "open_cycles": new_state.get("open_cycles", 0),
                "permanent": new_state.get("permanent", False),
                "cooldown_until": new_state.get("cooldown_until"),
            }
        return new_state


_SNAPSHOT: Optional[CircuitSnapshot] = None


def install_snapshot(ttl_sec: float = SNAPSHOT_TTL_SEC) -> CircuitSnapshot:
    """Install the snapshot as this forwarder's circuit read/write path.

    Patches ``circuit.is_selectable`` and ``circuit.record_outcome`` to consult
    the local snapshot. Localized to this forwarder process — the control plane
    does NOT install it (it reads proxy-redis directly, which is in-cluster for
    control). Idempotent: a second call is a no-op returning the existing
    snapshot.

    ``get_state`` and ``all_circuits`` are intentionally NOT patched: the
    write-path state machine must read live Redis, and ``/circuits`` is a cold
    monitoring endpoint where a live scan is correct.
    """
    global _SNAPSHOT
    if _SNAPSHOT is not None:
        return _SNAPSHOT
    snap = CircuitSnapshot(ttl_sec=ttl_sec)
    snap._orig_record_outcome = circuit.record_outcome  # capture before patching
    snap.start()
    circuit.is_selectable = snap.is_selectable  # type: ignore[assignment]
    circuit.record_outcome = snap.record_outcome  # type: ignore[assignment]
    _SNAPSHOT = snap
    logger.info("circuit snapshot installed (ttl=%ss) — hot-path reads are in-memory", ttl_sec)
    return snap


def get_snapshot() -> Optional[CircuitSnapshot]:
    """The installed snapshot, or None if ``install_snapshot`` hasn't run."""
    return _SNAPSHOT


def create_app():
    """Build the forwarder FastAPI app — the control app + snapshot installed.

    Same ``/acquire`` ``/release`` ``/health`` ``/providers`` ``/circuits``
    endpoints as the control plane; the only forwarder-specific behavior is the
    local circuit snapshot, installed once at process start (before uvicorn
    serves the first request).
    """
    install_snapshot()
    from fd_proxy_service.control.app import create_app as _create_control_app
    return _create_control_app()
