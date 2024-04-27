FROM rust:1.75-bookworm AS be

COPY . .

# Copy in source.
WORKDIR /be

RUN chmod +x ./docker.sh
RUN ./docker.sh

FROM debian:bookworm-slim


# Copy in built stuff.
COPY --from=be /be/target/release/hexa-be /
COPY --from=be /be/start.sh /

RUN chmod +x ./start.sh

EXPOSE 8888

CMD ["./start.sh"]

