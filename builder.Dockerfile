FROM docker.io/library/rust:1.69-alpine

ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"

ARG CARGO_CONFIG=$CARGO_HOME/config.toml

RUN apk update && apk add --no-cache build-base perl cmake && \
    rustup component add rustfmt clippy && \
    echo "[source.crates-io]" >> $CARGO_CONFIG && \
    echo "replace-with = 'rsproxy-sparse'" >> $CARGO_CONFIG && \
    echo "[source.rsproxy]" >> $CARGO_CONFIG && \
    echo "registry = 'https://rsproxy.cn/crates.io-index'" >> $CARGO_CONFIG && \
    echo "[source.rsproxy-sparse]" >> $CARGO_CONFIG && \
    echo "registry = 'sparse+https://rsproxy.cn/index/'" >> $CARGO_CONFIG && \
    echo "[registries.rsproxy]" >> $CARGO_CONFIG && \
    echo "index = 'https://rsproxy.cn/crates.io-index'" >> $CARGO_CONFIG && \
    echo "[net]" >> $CARGO_CONFIG && \
    echo "git-fetch-with-cli = true" >> $CARGO_CONFIG 
