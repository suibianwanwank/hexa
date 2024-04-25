FROM maven:3.6.3-openjdk-8 AS builder

COPY . .

WORKDIR /hexa/fe

RUN chmod +x ./package.sh
RUN ./package.sh


FROM openjdk:8-jre-alpine

COPY --from=builder /hexa/fe/main/target/libs/ /hexa-fe/lib/
COPY --from=builder /hexa/fe/main/src/main/resources/ /hexa-fe/etc/

CMD ["./start.sh"]