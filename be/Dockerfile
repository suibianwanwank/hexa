FROM rust:1.75-bookworm AS be

COPY . .

# Copy in source.
WORKDIR /hexa/be


RUN chmod +x ./docker.sh
RUN ./docker.sh

FROM debian:bookworm-slim


# Copy in built stuff.
COPY --from=be /hexa/be/target/release/hexa-be /be

CMD ["./start.sh"]

