# Harbormaster CDC consumer image. Build from the REPO ROOT:
#   docker build -f cdc/consumer/Dockerfile -t harbormaster-cdc-consumer .
# Config is env-driven (HM_KAFKA_BOOTSTRAP, HM_ONLINE_TABLE, ...); see
# cdc/consumer/service.py ConsumerConfig.
FROM python:3.14-slim-bookworm AS build
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/srv
WORKDIR /srv

# Refresh inherited OS packages so the published runtime carries the security
# fixes available from the selected Python slim base at build time. librdkafka
# still ships inside the confluent-kafka wheel, so no extra apt package is
# required for the application.
RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install \
    "confluent-kafka>=2.3" "boto3>=1.34" "redis>=5" \
    "pyiceberg[sql-sqlite]>=0.7" "pyarrow>=15" \
    "aws-msk-iam-sasl-signer-python>=1.0" \
    "structlog>=24.1" "prometheus-client>=0.20" \
    && mkdir -p /runtime-libs \
    && cp -aL \
        /lib/x86_64-linux-gnu/libbz2.so.1.0 \
        /lib/x86_64-linux-gnu/libcom_err.so.2 \
        /lib/x86_64-linux-gnu/libcrypt.so.1 \
        /lib/x86_64-linux-gnu/libcrypto.so.3 \
        /lib/x86_64-linux-gnu/libdb-5.3.so \
        /lib/x86_64-linux-gnu/libffi.so.8 \
        /lib/x86_64-linux-gnu/libgcc_s.so.1 \
        /lib/x86_64-linux-gnu/libgdbm.so.6 \
        /lib/x86_64-linux-gnu/libgssapi_krb5.so.2 \
        /lib/x86_64-linux-gnu/libk5crypto.so.3 \
        /lib/x86_64-linux-gnu/libkeyutils.so.1 \
        /lib/x86_64-linux-gnu/libkrb5.so.3 \
        /lib/x86_64-linux-gnu/libkrb5support.so.0 \
        /lib/x86_64-linux-gnu/liblzma.so.5 \
        /lib/x86_64-linux-gnu/libncursesw.so.6 \
        /lib/x86_64-linux-gnu/libnsl.so.2 \
        /lib/x86_64-linux-gnu/libpanelw.so.6 \
        /lib/x86_64-linux-gnu/libreadline.so.8 \
        /lib/x86_64-linux-gnu/libsqlite3.so.0 \
        /lib/x86_64-linux-gnu/libssl.so.3 \
        /lib/x86_64-linux-gnu/libstdc++.so.6 \
        /lib/x86_64-linux-gnu/libtinfo.so.6 \
        /lib/x86_64-linux-gnu/libtirpc.so.3 \
        /lib/x86_64-linux-gnu/libuuid.so.1 \
        /lib/x86_64-linux-gnu/libz.so.1 \
        /runtime-libs/

COPY cdc ./cdc

FROM gcr.io/distroless/base-debian12:nonroot
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/srv
WORKDIR /srv

# Keep only the Python runtime, installed wheels, and CDC source in the final
# image. The distroless runtime deliberately excludes unused package-manager
# tooling and Perl, which removes the inherited critical CVE surface.
COPY --from=build --chown=nonroot:nonroot /usr/local /usr/local
COPY --from=build /runtime-libs/ /lib/x86_64-linux-gnu/
COPY --from=build --chown=nonroot:nonroot /srv/cdc /srv/cdc
USER nonroot:nonroot

CMD ["python", "-m", "cdc.consumer.service"]
