FROM rust:1.75-bookworm AS builder

# Copy in source.
WORKDIR /hexa
COPY . .

WORKDIR /hexa/be
RUN cargo build --release


FROM debian:bookworm-slim


# Copy in built stuff.
COPY --from=builder /hexa/be/target/release/hexa-be-server .
COPY --from=builder /hexa/be/target/release/hexa-cli .

CMD ["./hexa-be-server"]
CMD ["./hexa-cli"]