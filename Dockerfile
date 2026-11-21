FROM --platform=linux/amd64 python:3.12-slim-trixie AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ make \
 && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir --only-binary=:all: --no-deps pyarrow==26.0.1
WORKDIR /src
COPY Makefile ./
COPY src/ ./src/
RUN make local \
 && mkdir -p /release/lib \
 && cp build/shopping-sessions /release/ \
 && strip /release/shopping-sessions \
 && cp -L build/lib/*.so.* /release/lib/ \
 && mkdir -p /release/licenses \
 && cp /usr/local/lib/python3.12/site-packages/pyarrow-*.dist-info/licenses/*.txt /release/licenses/

FROM --platform=linux/amd64 debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends libstdc++6 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /release/ /opt/shopping-sessions/
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24
USER 1000:1000
ENTRYPOINT ["/opt/shopping-sessions/shopping-sessions"]
