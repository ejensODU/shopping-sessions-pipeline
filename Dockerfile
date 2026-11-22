FROM gcc:16-trixie@sha256:5c2f83335c97e233c80d2bc754bd5c46b2d9c32f236a765a4ac40a59f216c064 AS build
WORKDIR /src
COPY Makefile ./
COPY src/ ./src/
RUN make static \
 && mkdir -p /rootfs/data /rootfs/output /rootfs/scratch /rootfs/tmp \
 && chmod 1777 /rootfs/tmp

FROM scratch
COPY --from=build /rootfs/ /
COPY --from=build /src/build/shopping-sessions /shopping-sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
