FROM docker.io/library/rust:1.69-alpine

RUN apk update && apk add --no-cache build-base perl cmake && \
    rustup component add rustfmt clippy
