FROM rust:1.75-bookworm AS be

# Copy in source.
WORKDIR /hexa

COPY . .

RUN chmod +x /hexa/docker.sh
RUN ./docker.sh

FROM debian:bookworm-slim


# Copy in built stuff.
COPY --from=be /hexa/be/target/release/hexa-be-server /be
COPY --from=be /hexa/be/target/release/hexa-cli /be
COPY --from=fe /hexa/fe/lib /fe/lib


CMD ["./start.sh"]

