FROM rust:1.75-bookworm AS be

COPY . .

# Copy in source.
WORKDIR /hexa/cli


RUN chmod +x ./docker.sh
RUN ./docker.sh

FROM debian:bookworm-slim


# Copy in built stuff.
COPY --from=be /hexa/cli/target/release/cli /cli

CMD ["./start.sh"]

