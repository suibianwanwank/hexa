FROM maven:3.6.3-openjdk-8 AS builder

COPY . .

WORKDIR /hexa/fe

RUN chmod +x ./package.sh
RUN ./package.sh


FROM openjdk:8-jre-alpine

COPY --from=builder /hexa/fe/main/target/libs/ /hexa-fe/lib/
COPY --from=builder /hexa/fe/main/target/main-1.0-SNAPSHOT.jar /hexa-fe/lib/
COPY --from=builder /hexa/fe/main/src/main/resources/ /hexa-fe/etc/
COPY --from=builder /hexa/fe/start.sh /hexa-fe/bin/

RUN chmod +x /hexa-fe/bin/start.sh
EXPOSE 9065 8082

CMD ["/hexa-fe/bin/start.sh"]