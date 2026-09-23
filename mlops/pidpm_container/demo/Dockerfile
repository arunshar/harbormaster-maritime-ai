# Phase 3 AWS-showcase DEMO STAND-IN container. NOT Pi-DPM: see server.py's
# module docstring. Builds and runs today with no external dependencies
# (unlike ../Dockerfile, which needs the scorer code from a separate,
# un-vendored earlier reinforcement-learning repository of mine); this is what docs/runbooks/PHASE_3_AWS_SHOWCASE.md
# actually deploys to demonstrate the real SageMaker async endpoint
# infrastructure, scale-to-zero, and the promotion pipeline.

FROM python:3.14-slim-bookworm AS build

WORKDIR /opt/pidpm-demo

RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

COPY server.py entrypoint.py /opt/pidpm-demo/

RUN pip install --no-cache-dir "flask>=3.0" "gunicorn>=22.0"

# Copy only the shared runtime libraries CPython needs into the distroless
# Debian 12 image. This keeps package-manager and Perl attack surface out of
# the deployed inference image.
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

WORKDIR /opt/pidpm-demo

COPY --from=build /usr/local /usr/local
COPY --from=build /runtime-libs/ /lib/x86_64-linux-gnu/
COPY --from=build --chown=nonroot:nonroot /opt/pidpm-demo /opt/pidpm-demo

USER nonroot:nonroot

ENV MODEL_PATH=/opt/ml/model/checkpoint.json
EXPOSE 8080

# SageMaker's contract: /ping and /invocations on port 8080. The Python
# entrypoint ignores SageMaker's trailing `serve` argument and starts Gunicorn.
ENTRYPOINT ["/usr/local/bin/python", "/opt/pidpm-demo/entrypoint.py"]
