# Harbormaster replay-ingestor image. Build from the REPO ROOT:
#   docker build -f streaming/ingestor/Dockerfile -t harbormaster-ingestor .
# The fixture is read from S3 at runtime (FIXTURE_URI), not bundled into the image.
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/srv/streaming
WORKDIR /srv

RUN pip install --upgrade pip && pip install "boto3>=1.34" "pydantic>=2.6"

# The ingestor imports replay.loader (schema + AisRecord) and ingestor.ingest.
COPY streaming/replay ./streaming/replay
COPY streaming/ingestor ./streaming/ingestor

# REPLAY mode by default; needs KINESIS_STREAM_ARN (or _NAME) + FIXTURE_URI at runtime.
CMD ["python", "-m", "ingestor.ingest"]
