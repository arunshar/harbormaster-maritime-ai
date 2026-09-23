"""The canary weight actuator (Phase 3, gate 3.7): the real
set_canary_weight / revert_to_champion callables that run_promotion
(mlops/promote.py) injects at its I/O boundary.

run_promotion stays a pure state machine over injected callables; this
module is where those callables meet SageMaker. Both factories close over an
injected client (this repo's pattern everywhere else, see mlops/registry.py
and serving/app/pidpm_client.py): unit tests use a fake, never real
boto3/AWS. Weight shifts go through the UpdateEndpointWeightsAndCapacities
API on an endpoint that supports multiple production variants. The current
Phase 3 SageMaker Async Inference endpoint intentionally has one variant,
because AWS rejects multi-variant async configurations. This actuator remains
valid for a separately reviewed real-time or dual-endpoint candidate design.

Fail-closed, deliberately the opposite of pidpm_client's fail-open: a
scoring-path outage degrades to the analytic estimate, but a promotion that
cannot move traffic must STOP. Any client exception propagates unswallowed
to run_promotion's caller; there is no retry or logging layer here to hide a
half-applied ramp.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol


class SageMakerEndpointWeightsClient(Protocol):
    def update_endpoint_weights_and_capacities(self, **kwargs: Any) -> dict: ...


def make_set_canary_weight(
    client: SageMakerEndpointWeightsClient,
    endpoint_name: str,
    champion: str = "champion",
    candidate: str = "candidate",
) -> Callable[[int], None]:
    """Build the ``set_canary_weight(weight)`` callable for ``run_promotion``.

    ``weight`` is a whole percentage from CANARY_WEIGHTS (5/25/50/100). One
    API call moves BOTH variants (candidate to weight/100, champion to the
    complement), so the pair always sums to 1.0 and there is no intermediate
    state where the endpoint over- or under-serves. The default variant
    names must belong to an endpoint that supports multiple production
    variants. Do not use this factory against the Phase 3 async endpoint.
    """

    def set_canary_weight(weight: int) -> None:
        fraction = weight / 100
        client.update_endpoint_weights_and_capacities(
            EndpointName=endpoint_name,
            DesiredWeightsAndCapacities=[
                {"VariantName": candidate, "DesiredWeight": fraction},
                {"VariantName": champion, "DesiredWeight": 1 - fraction},
            ],
        )

    return set_canary_weight


def make_revert_to_champion(
    client: SageMakerEndpointWeightsClient,
    endpoint_name: str,
    champion: str = "champion",
    candidate: str = "candidate",
) -> Callable[[], None]:
    """Build the ``revert_to_champion()`` callable for ``run_promotion``.

    Invariant 3 (docs/phases/PHASE_3.md): a breach reverts the FULL weight
    to the champion in ONE action. This is that one action, a single
    UpdateEndpointWeightsAndCapacities call restoring champion 1.0 and
    candidate 0.0, which either succeeds atomically or raises; never a
    step-down ramp.
    """

    def revert_to_champion() -> None:
        client.update_endpoint_weights_and_capacities(
            EndpointName=endpoint_name,
            DesiredWeightsAndCapacities=[
                {"VariantName": champion, "DesiredWeight": 1.0},
                {"VariantName": candidate, "DesiredWeight": 0.0},
            ],
        )

    return revert_to_champion
