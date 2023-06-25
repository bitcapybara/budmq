FROM docker.io/library/rust:1.70-alpine

RUN apk add --no-cache build-base perl cmake && \
    rustup component add rustfmt clippy
