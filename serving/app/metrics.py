"""Prometheus metrics for the serving plane.

Mirrors the shape of MIRROR's serving/metrics.py: counters and a latency
histogram exposed at /metrics, scraped (or shipped via CloudWatch EMF in 1.7).
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram

SCORES_TOTAL = Counter("hm_scores_total", "AIS score requests handled")
ANOMALIES_TOTAL = Counter("hm_anomalies_total", "Score responses carrying >= 1 anomaly reason")
HITL_ENQUEUED_TOTAL = Counter("hm_hitl_enqueued_total", "Events enqueued for human review")
REASONS_TOTAL = Counter("hm_reasons_total", "Anomaly reasons emitted, by code", ["code"])
REJECTS_TOTAL = Counter("hm_rejects_total", "Requests rejected, by reason", ["reason"])
SCORE_LATENCY = Histogram(
    "hm_score_latency_seconds",
    "End-to-end /v1/score-ais latency",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0),
)

# Phase 2 (gate C2): the CDC-fed online watchlist read path.
WATCHLIST_HITS_TOTAL = Counter(
    "hm_watchlist_hits_total", "Scored events whose vessel is watchlisted or sanctioned"
)
WATCHLIST_LOOKUP_ERRORS = Counter(
    "hm_watchlist_lookup_errors_total",
    "Online watchlist lookups that failed (the scorer fails open and scores anyway)",
)

# Phase 3 (gate 3.6): the SageMaker async Pi-DPM scoring path.
PIDPM_LOOKUP_ERRORS = Counter(
    "hm_pidpm_lookup_errors_total",
    "Pi-DPM SageMaker calls that failed or timed out (falls back to the analytic estimate)",
)
