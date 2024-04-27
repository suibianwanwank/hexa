FROM maven:3.6.3-openjdk-8 AS builder

COPY . .

WORKDIR /fe

RUN chmod +x ./package.sh
RUN ./package.sh


FROM openjdk:8-jre-slim-buster

WORKDIR /fe

COPY --from=builder /fe/main/target/libs/ ./lib/
COPY --from=builder /fe/main/target/main-1.0-SNAPSHOT.jar ./lib/
COPY --from=builder /fe/main/src/main/resources/config.properties ./etc/
COPY --from=builder /fe/start.sh ./bin/

EXPOSE 9065 8082


RUN ls
RUN pwd

# 设置环境变量

WORKDIR /fe/lib


RUN ls
RUN pwd


# 运行 Java 应用程序
CMD ["java", "-Dconfig=/fe/etc/config.properties", "-cp", "*", "com.ccsu.server.Launcher"]