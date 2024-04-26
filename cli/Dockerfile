FROM rust:1.75-bookworm AS builder

COPY . .

# Copy in source.
WORKDIR /hexa/cli


RUN chmod +x ./docker.sh
RUN ./docker.sh

FROM debian:bookworm-slim


# Copy in built stuff.
COPY --from=builder /hexa/cli/target/release/cli /cli
COPY --from=builder /hexa/cli/start.sh /

RUN chmod +x ./start.sh

CMD ["./start.sh"]

