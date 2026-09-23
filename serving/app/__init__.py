"""Harbormaster serving plane.

The deterministic AIS anomaly-scoring front door. The geometric kernel and the
deterministic agents are vendored from GeoTrace-Agent. A private planning note,
which is not part of this repository, names the reuse anchors. A deterministic
HeuristicPlanner and a fixed scoring fusion replace the LLM planner and
summarizer, so the live path costs zero tokens. In the design, heavy models
(Pi-DPM) are trained on an off-cloud GPU cluster. Here the gap scorer uses the
numpy surrogate.
"""

__version__ = "0.1.0"
