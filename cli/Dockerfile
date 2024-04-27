FROM rust:1.75-bookworm AS builder

COPY . .

# Copy in source.
WORKDIR /cli


RUN chmod +x ./docker.sh
RUN ./docker.sh

FROM debian:bookworm-slim


# Copy in built stuff.
COPY --from=builder /cli/target/release/cli /cli
COPY --from=builder /cli/start.sh /

RUN chmod +x ./start.sh

CMD ["./start.sh"]

