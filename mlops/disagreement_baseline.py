"""Phase 4 gate 4.3: deriving the concept-drift alert threshold from data.

mlops/drift_decision.py compares the HITL disagreement rate against
DISAGREEMENT_ALERT_RATE_PLACEHOLDER, a named placeholder whose TODO demands a
threshold derived from a real accumulated HITL disagreement-rate baseline.
This module is that derivation path. It does NOT supply an empirical number:
it supplies the mechanism, and every constant below is a documented design
choice, not a measured result. The threshold only becomes real once real
HITL windows have accumulated through DisagreementBaseline.

The baseline is a sequence of BaselineWindow(rate, n) records, one per
evaluation window, each the (rate, n) of a mlops.concept_proxy.
disagreement_rate result over that window's labeled hitl_queue rows.
derive_alert_threshold turns the accumulated windows into one threshold:

    threshold = max(q95, MEAN_MULTIPLIER * mean, RATE_FLOOR)

computed over the usable windows (those with n >= min_window_n). Each term
is a deliberate choice:

- q95: the nearest-rank 95th percentile of usable window rates, i.e. the
  ceil(0.95 * k)-th smallest of k rates, with the rank computed in integer
  arithmetic so float rounding can never shift it. Alerting above q95 means
  a new window must beat all but the top 5% of the healthy baseline before
  it counts as concept drift. Nearest-rank is used instead of interpolation
  because at the window counts this gate will actually see (five to a few
  dozen) interpolated precision would be spurious; for fewer than 20 usable
  windows the nearest-rank q95 is simply the maximum observed rate, which
  is the honest reading of that little history.
- MEAN_MULTIPLIER * mean: on a flat or very quiet baseline q95 sits at or
  barely above the typical rate, so q95 alone would alert on a small
  absolute wiggle. Requiring at least a doubling of the mean window rate
  encodes: the typical disagreement rate has to double before this gate
  calls it concept drift.
- RATE_FLOOR: one overridden verdict in a 20-row window already yields a
  rate of 0.05, so any threshold below 0.05 would alert on single-label
  quantization noise. The floor matches the smallest usable window
  (min_window_n = 20, mirroring calibration_watch.DEFAULT_MIN_LABELED).

Windows with n < min_window_n are excluded from the derivation entirely:
their rates are quantized in steps of 1/n >= 5% and would distort both the
percentile and the mean. Fewer than min_windows usable windows is an
insufficient-data guard, same convention as calibration_watch: below it,
derive_alert_threshold refuses (ValueError) and make_disagreement_alert_rate
returns the caller-supplied fallback (the named placeholder by default), so
classify_drift keeps running on the testable placeholder until real HITL
data exists. Persistence of the windows is the caller's job (no I/O here,
matching the convention used throughout this repo).
"""

from __future__ import annotations

from dataclasses import dataclass

from mlops.drift_decision import DISAGREEMENT_ALERT_RATE_PLACEHOLDER

DEFAULT_MIN_WINDOWS = 5
DEFAULT_MIN_WINDOW_N = 20
# q95 kept as the exact fraction 19/20 so the nearest-rank index is pure
# integer arithmetic (ceil(19k/20)), immune to float rounding of 0.95 * k.
TAIL_NUMERATOR = 19
TAIL_DENOMINATOR = 20
MEAN_MULTIPLIER = 2.0
RATE_FLOOR = 0.05


@dataclass(frozen=True)
class BaselineWindow:
    """One evaluation window of the accumulated HITL baseline: the
    disagreement rate over that window's labeled rows, and how many labeled
    rows produced it (a DisagreementResult's rate and n)."""

    rate: float
    n: int


class DisagreementBaseline:
    """Append-only accumulator of BaselineWindow records. Holds the windows
    and answers which are usable for derivation; storage is the caller's."""

    def __init__(self) -> None:
        self._windows: list[BaselineWindow] = []

    @property
    def windows(self) -> tuple[BaselineWindow, ...]:
        return tuple(self._windows)

    def append(self, window: BaselineWindow) -> None:
        if not 0.0 <= window.rate <= 1.0:
            raise ValueError(f"window rate must be in [0, 1], got {window.rate}")
        if window.n < 0:
            raise ValueError(f"window n must be >= 0, got {window.n}")
        self._windows.append(window)

    def usable_windows(self, *, min_window_n: int = DEFAULT_MIN_WINDOW_N) -> list[BaselineWindow]:
        """Windows with enough labeled rows to trust their rate; a tiny-n
        window's rate is quantized in steps of 1/n and is excluded rather
        than allowed to distort the percentile or the mean."""
        return [w for w in self._windows if w.n >= min_window_n]


def _nearest_rank_q95(sorted_rates: list[float]) -> float:
    """The ceil(0.95 * k)-th smallest of k sorted rates, rank computed as
    (19k + 19) // 20 in integer arithmetic (see module docstring)."""
    k = len(sorted_rates)
    rank = (TAIL_NUMERATOR * k + TAIL_DENOMINATOR - 1) // TAIL_DENOMINATOR
    return sorted_rates[rank - 1]


def derive_alert_threshold(
    baseline: DisagreementBaseline,
    *,
    min_windows: int = DEFAULT_MIN_WINDOWS,
    min_window_n: int = DEFAULT_MIN_WINDOW_N,
) -> float:
    """max(q95, MEAN_MULTIPLIER * mean, RATE_FLOOR) over the usable windows;
    every term is justified in the module docstring. Refuses (ValueError)
    below min_windows usable windows: a percentile of less history than that
    is not a baseline. Callers wanting a graceful fallback go through
    make_disagreement_alert_rate instead."""
    usable = baseline.usable_windows(min_window_n=min_window_n)
    if len(usable) < min_windows:
        raise ValueError(
            f"need >= {min_windows} usable windows (n >= {min_window_n}) "
            f"to derive a threshold, got {len(usable)}"
        )
    rates = sorted(w.rate for w in usable)
    q95 = _nearest_rank_q95(rates)
    mean = sum(rates) / len(rates)
    return max(q95, MEAN_MULTIPLIER * mean, RATE_FLOOR)


def make_disagreement_alert_rate(
    baseline: DisagreementBaseline,
    *,
    fallback: float = DISAGREEMENT_ALERT_RATE_PLACEHOLDER,
    min_windows: int = DEFAULT_MIN_WINDOWS,
    min_window_n: int = DEFAULT_MIN_WINDOW_N,
) -> float:
    """The seam callers pass into classify_drift's disagreement_alert_rate
    parameter: the derived threshold once enough real windows exist, the
    named placeholder fallback until then. The fallback path is exactly the
    sprint-honest status quo, so wiring this in changes nothing until real
    HITL data has accumulated."""
    if len(baseline.usable_windows(min_window_n=min_window_n)) < min_windows:
        return fallback
    return derive_alert_threshold(baseline, min_windows=min_windows, min_window_n=min_window_n)
