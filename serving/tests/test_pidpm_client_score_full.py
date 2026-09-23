"""Gate 4.3: PiDpmClient's additive {"score", "epistemic_variance"} contract.

Reuses the fakes from test_pidpm_client.py's style rather than importing
them (that file stays untouched and its own tests keep proving the
pre-Phase-4 float | None contract is byte-for-byte preserved).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from app.pidpm_client import PiDpmClient, PiDpmScore


class NoSuchKey(Exception):
    pass


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

        class _Exceptions:
            NoSuchKey = NoSuchKey

        self.exceptions = _Exceptions()

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        pass

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        if (Bucket, Key) not in self.objects:
            raise self.exceptions.NoSuchKey()
        return {"Body": _Body(self.objects[(Bucket, Key)])}

    def seed_output(self, bucket: str, key: str, payload: dict) -> None:
        self.objects[(bucket, key)] = json.dumps(payload).encode()


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeSageMaker:
    def __init__(self, *, output_location: str) -> None:
        self.output_location = output_location

    def invoke_endpoint_async(self, **kwargs: Any) -> dict:
        return {"OutputLocation": self.output_location}


def _client(sagemaker, s3) -> PiDpmClient:
    return PiDpmClient(
        sagemaker_client=sagemaker,
        s3_client=s3,
        endpoint_name="hm-pidpm",
        input_bucket="hm-models-bucket",
        sleep=lambda _s: None,
    )


def test_new_shape_payload_returns_score_and_variance_via_score_full():
    s3 = FakeS3()
    s3.seed_output(
        "hm-models-bucket",
        "pidpm/async-output/result.json",
        {"score": 0.82, "epistemic_variance": 0.15},
    )
    sagemaker = FakeSageMaker(output_location="s3://hm-models-bucket/pidpm/async-output/result.json")
    client = _client(sagemaker, s3)

    result = client.score_full([[40.0, -74.0]])

    assert result == PiDpmScore(score=0.82, epistemic_variance=0.15)


def test_old_shape_payload_through_score_full_has_none_variance():
    s3 = FakeS3()
    s3.seed_output("hm-models-bucket", "pidpm/async-output/result.json", {"score": 0.73})
    sagemaker = FakeSageMaker(output_location="s3://hm-models-bucket/pidpm/async-output/result.json")
    client = _client(sagemaker, s3)

    result = client.score_full([[40.0, -74.0]])

    assert result == PiDpmScore(score=0.73, epistemic_variance=None)


def test_new_shape_payload_through_old_score_method_still_returns_bare_float():
    s3 = FakeS3()
    s3.seed_output(
        "hm-models-bucket",
        "pidpm/async-output/result.json",
        {"score": 0.91, "epistemic_variance": 0.02},
    )
    sagemaker = FakeSageMaker(output_location="s3://hm-models-bucket/pidpm/async-output/result.json")
    client = _client(sagemaker, s3)

    assert client.score([[40.0, -74.0]]) == 0.91


def test_ascore_full_runs_off_the_event_loop_and_matches_score_full():
    s3 = FakeS3()
    s3.seed_output(
        "hm-models-bucket",
        "pidpm/async-output/result.json",
        {"score": 0.5, "epistemic_variance": 0.3},
    )
    sagemaker = FakeSageMaker(output_location="s3://hm-models-bucket/pidpm/async-output/result.json")
    client = _client(sagemaker, s3)

    result = asyncio.run(client.ascore_full([[40.0, -74.0]]))

    assert result == PiDpmScore(score=0.5, epistemic_variance=0.3)


def test_score_full_returns_none_when_disabled():
    client = PiDpmClient(sagemaker_client=None, s3_client=None, endpoint_name="", input_bucket="")
    assert client.score_full([[1.0, 2.0]]) is None


def test_ascore_full_returns_none_when_disabled():
    client = PiDpmClient(sagemaker_client=None, s3_client=None, endpoint_name="", input_bucket="")
    assert asyncio.run(client.ascore_full([[1.0, 2.0]])) is None
