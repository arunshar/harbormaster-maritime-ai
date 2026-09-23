"""Phase 5 gate 5.7: the PPO route-optimization STRETCH service.

A labeled stretch, behind the
``enable_phase5_ppo_stretch`` toggle (default false, INDEPENDENT of
``enable_phase5``), shipped as its own FastAPI microservice
(``mlops/route_optimizer/service.py``). It is NEVER imported by
``serving/app/orchestrator.py``, never wired into the promotion pipeline
(``mlops/promote.py``), and never counted toward the gap-closing
claim in ``docs/HONESTY.md`` (a test in
``mlops/tests/test_route_optimizer_service.py`` pins the isolation).

Composition:
- ``vendored_kl``: the ``AdaptiveKLController`` (and ``cosine_lr``) from
  an earlier reinforcement-learning repository of mine, vendored with a provenance header,
  not imported.
- ``ppo``: the ``PpoTrainer``/``PpoConfig`` SHAPE from the earlier RL
  repository's ``ppo_trainer.py``, retargeted from LM fine-tuning to a tabular numpy
  policy over the corridor graph (zero GPU, zero torch).
- ``graph``/``reward``: the Phase 3 ``corridor_graph_nodes`` /
  ``corridor_graph_edges`` Iceberg tables as the action space, with
  ``coverage_minus_fuel`` as the reward.
- ``feasibility``: the EXISTING S-KBM kinematic gate
  (``serving/app/agents/validator.py``), reused, not rebuilt.
- ``rollout``: tiny-synthetic-graph rollouts feeding one CPU training step
  (``make ppo-stretch-smoke``).
"""
