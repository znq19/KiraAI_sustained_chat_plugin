"""Adaptive, bounded debounce windows learned from user arrival times.

The module deliberately has no KiraAI imports and performs no I/O.  Hosts own the
actual asyncio tasks and buffers; this class only learns intervals and supplies
windows/deadlines.  All timestamps are monotonic seconds.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class _Profile:
    samples: deque = field(default_factory=lambda: deque(maxlen=32))
    last_arrival: float | None = None
    boost_window: float = 0.0
    boost_until: float = 0.0
    touched: float = 0.0


class AdaptiveMergePolicy:
    """Recent-gap quantile + expiring near-miss compensation.

    ``base_getter`` is called on demand so WebUI edits never require a plugin
    reload.  Runtime values are never written back to the global config.
    """

    SAMPLE_HALF_LIFE = 600.0
    BOOST_TTL = 600.0
    MIN_SAMPLES = 4
    MAX_PROFILES = 2048
    MAX_SESSIONS = 2048

    def __init__(self, base_getter, cfg: dict):
        self._base_getter = base_getter
        self.enabled = bool(cfg.get("adaptive_merge_enabled", True))
        mode = str(cfg.get("adaptive_merge_mode", "balanced") or "balanced").strip().lower()
        self.mode = mode if mode in ("responsive", "balanced", "merge_first") else "balanced"
        self.max_window_override = self._nonnegative(cfg.get("adaptive_merge_max_seconds", 0))
        self.max_batch_wait_override = self._nonnegative(cfg.get("adaptive_merge_max_batch_wait", 0))
        self.allow_below_base = bool(cfg.get("adaptive_merge_allow_below_base", False))
        self.min_window_override = self._nonnegative(cfg.get("adaptive_merge_min_seconds", 0))
        self._profiles: dict[tuple[str, str], _Profile] = {}
        # sid -> [base used, current window, anchor sender]
        self._windows: dict[str, list] = {}
        self._last_flush: dict[str, dict] = {}

    @staticmethod
    def _nonnegative(value) -> float:
        try:
            number = float(value)
            return number if math.isfinite(number) and number >= 0 else 0.0
        except (TypeError, ValueError):
            return 0.0

    def base_interval(self) -> float:
        try:
            value = float(self._base_getter())
            if math.isfinite(value) and value > 0:
                return value
        except (TypeError, ValueError, AttributeError):
            pass
        return 1.5

    def min_interval(self, base: float | None = None) -> float:
        base = self.base_interval() if base is None else base
        if not self.allow_below_base:
            return base
        floor = self.min_window_override if self.min_window_override > 0 else base * 0.5
        return min(base, max(0.1, floor))

    def max_interval(self, base: float | None = None) -> float:
        base = self.base_interval() if base is None else base
        ceiling = self.max_window_override if self.max_window_override > 0 else base * 3.0
        return max(base, ceiling)

    def max_batch_wait(self, base: float | None = None) -> float:
        base = self.base_interval() if base is None else base
        ceiling = self.max_batch_wait_override if self.max_batch_wait_override > 0 else base * 4.0
        return max(self.max_interval(base), ceiling)

    def margin(self, base: float | None = None) -> float:
        base = self.base_interval() if base is None else base
        return min(0.3, max(0.1, base * 0.05))

    def near_miss_grace(self, base: float | None = None) -> float:
        base = self.base_interval() if base is None else base
        return min(1.5, base * 0.3)

    @property
    def quantile(self) -> float:
        return {"responsive": 0.60, "balanced": 0.75, "merge_first": 0.90}[self.mode]

    @staticmethod
    def _weighted_quantile(samples, now: float, quantile: float):
        weighted = []
        total = 0.0
        for ts, gap in samples:
            age = max(0.0, now - ts)
            weight = 2.0 ** (-age / AdaptiveMergePolicy.SAMPLE_HALF_LIFE)
            if weight <= 0:
                continue
            weighted.append((gap, weight))
            total += weight
        if not weighted or total <= 0:
            return None, 0.0
        weighted.sort(key=lambda item: item[0])
        threshold = total * quantile
        cumulative = 0.0
        for gap, weight in weighted:
            cumulative += weight
            if cumulative >= threshold:
                return gap, min(16.0, total)
        return weighted[-1][0], min(16.0, total)

    def _profile(self, key: tuple[str, str], now: float) -> _Profile:
        profile = self._profiles.get(key)
        if profile is None:
            profile = _Profile()
            self._profiles[key] = profile
        profile.touched = now
        return profile

    def _prune(self) -> None:
        if len(self._profiles) > self.MAX_PROFILES:
            excess = len(self._profiles) - self.MAX_PROFILES
            oldest = sorted(self._profiles, key=lambda k: self._profiles[k].touched)[:excess]
            for key in oldest:
                self._profiles.pop(key, None)
        for mapping in (self._windows, self._last_flush):
            while len(mapping) > self.MAX_SESSIONS:
                mapping.pop(next(iter(mapping)), None)

    def observe(self, sid: str, sender_id, now: float | None = None) -> None:
        """Learn one real user arrival and apply an expiring near-miss boost."""
        if not self.enabled or not sid or sender_id is None:
            return
        now = time.monotonic() if now is None else now
        uid = str(sender_id)
        base = self.base_interval()
        key = (sid, uid)
        profile = self._profile(key, now)

        previous_flush = self._last_flush.get(sid)
        if previous_flush and previous_flush.get("reason") == "timeout" \
                and previous_flush.get("sender_id") == uid:
            late = now - previous_flush["flushed_at"]
            raw_gap = now - previous_flush["last_message_at"]
            if 0 <= late <= self.near_miss_grace(base) and raw_gap > 0:
                boost = min(self.max_interval(base), raw_gap + self.margin(base))
                if boost >= profile.boost_window or now >= profile.boost_until:
                    profile.boost_window = boost
                profile.boost_until = now + self.BOOST_TTL

        previous_arrival = profile.last_arrival
        if previous_arrival is not None:
            gap = now - previous_arrival
            # Only actionable, same-sender gaps are learned.  Long gaps usually
            # belong to a new conversation and must not inflate the window.
            if 0 < gap <= max(10.0, self.max_interval(base)):
                profile.samples.append((now, gap))
                pooled = self._profile((sid, "*"), now)
                pooled.samples.append((now, gap))
        profile.last_arrival = now
        self._prune()

    def start_batch(self, sid: str, sender_id=None, now: float | None = None) -> float:
        """Compute the window for a new batch; callers retain it until flush."""
        now = time.monotonic() if now is None else now
        base = self.base_interval()
        if not self.enabled:
            self._windows[sid] = [base, base, None if sender_id is None else str(sender_id)]
            return base
        uid = None if sender_id is None else str(sender_id)
        user = self._profiles.get((sid, uid)) if uid is not None else None
        pooled = self._profiles.get((sid, "*"))
        selected = None
        if user is not None and len(user.samples) >= self.MIN_SAMPLES:
            selected = user
        elif pooled is not None and len(pooled.samples) >= self.MIN_SAMPLES:
            selected = pooled

        if selected is None:
            target = base
        else:
            learned, effective_n = self._weighted_quantile(selected.samples, now, self.quantile)
            if learned is None:
                target = base
            else:
                # Four pseudo-samples anchored at the configured base prevent a
                # handful of observations from causing abrupt window changes.
                shrunk = (effective_n * learned + 4.0 * base) / (effective_n + 4.0)
                target = shrunk + self.margin(base)

        lower, upper = self.min_interval(base), self.max_interval(base)
        target = min(upper, max(lower, target))
        previous = self._windows.get(sid)
        # 用户改基准是显式配置变更，不是学习噪声：以新基准重新锚定，
        # 不对新旧两个基准做 70/30 平滑。
        old = previous[1] if previous is not None and previous[0] == base else base
        window = 0.7 * old + 0.3 * target

        # Near-miss evidence belongs to the individual, never to unrelated
        # group participants.  It can react faster than the ordinary smoother.
        if user is not None and now < user.boost_until and user.boost_window > 0:
            window = max(window, user.boost_window)
        window = min(upper, max(lower, window))
        self._windows[sid] = [base, window, uid]
        self._prune()
        return window

    def current_window(self, sid: str, sender_id=None, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        base = self.base_interval()
        previous = self._windows.get(sid)
        if previous is None or previous[0] != base:
            return self.start_batch(sid, sender_id, now)
        return previous[1]

    def timing(self, sid: str, first_at: float, last_at: float,
               sender_id=None, now: float | None = None):
        """Return (deadline, reason, window), applying a total cap only when on."""
        now = time.monotonic() if now is None else now
        window = self.current_window(sid, sender_id, now)
        quiet_deadline = last_at + window
        if not self.enabled:
            return quiet_deadline, "timeout", window
        total_deadline = first_at + self.max_batch_wait()
        if total_deadline <= quiet_deadline:
            return total_deadline, "max_wait", window
        return quiet_deadline, "timeout", window

    def note_flush(self, sid: str, sender_id=None, last_message_at: float | None = None,
                   reason: str = "external", now: float | None = None) -> None:
        """Record batch formation time; LLM queueing/latency is intentionally excluded."""
        now = time.monotonic() if now is None else now
        self._last_flush[sid] = {
            "sender_id": None if sender_id is None else str(sender_id),
            "last_message_at": now if last_message_at is None else last_message_at,
            "flushed_at": now,
            "reason": str(reason or "external"),
        }
        self._prune()

    def clear_flush(self, sid: str) -> None:
        self._last_flush.pop(sid, None)

    def snapshot(self, sid: str) -> dict:
        base = self.base_interval()
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "base": base,
            "window": self.current_window(sid),
            "min": self.min_interval(base),
            "max": self.max_interval(base),
            "max_batch_wait": self.max_batch_wait(base),
            "allow_below_base": self.allow_below_base,
        }
