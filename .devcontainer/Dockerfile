ARG VARIANT="bookworm"
FROM rust:1-${VARIANT}

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive
RUN apt install docker