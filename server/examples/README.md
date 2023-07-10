## server example

### start server with local memory storage

```bash
cargo run --example server
```

### start server with bonsaidb persist storage

```bash
cargo run --example server --features bonsaidb
```

### start server with redis and mongodb storage

```bash
cargo run --example server --feature redis,mongodb
```
