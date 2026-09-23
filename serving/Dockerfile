# Harbormaster serving plane image. Build from the REPO ROOT (needs pyproject +
# the serving/streaming package dirs):
#   docker build -f serving/Dockerfile -t harbormaster-serving .
FROM python:3.14-slim-bookworm AS build
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /srv

# Refresh the build stage before installing the serving runtime dependencies.
RUN apt-get update \
 && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

# Deps + package. The serving runtime extra supplies the configured Postgres,
# DynamoDB, and Redis paths. The wheel carries cdc.schema because registry and
# HITL initialization use its DDL. Shapely wheels bundle GEOS.
COPY pyproject.toml README.md ./
COPY serving/app ./serving/app
COPY streaming/features ./streaming/features
COPY cdc ./cdc
RUN pip install --upgrade pip && pip install ".[serving-runtime]"

# Preserve the Debian shared libraries required by CPython and its extension
# modules. Compiled Python wheels and their bundled libraries remain under
# /usr/local, while the final Debian 12 base supplies the loader and libc.
RUN mkdir -p /runtime-libs \
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

FROM gcr.io/distroless/base-debian12:nonroot
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/srv
WORKDIR /srv

COPY --from=build /usr/local /usr/local
COPY --from=build /runtime-libs/ /lib/x86_64-linux-gnu/
COPY --from=build --chown=nonroot:nonroot /srv/cdc /srv/cdc
USER nonroot:nonroot

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
