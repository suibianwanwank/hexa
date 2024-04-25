#!/bin/bash

# protoc env
wget https://github.com/protocolbuffers/protobuf/releases/download/v26.1/protoc-26.1-linux-x86_64.zip

unzip protoc-26.1-linux-x86_64.zip -d protoc

export PATH="$PWD/protoc/bin:$PATH"

protoc --version

cd /hexa/fe

## download mvn
#sudo apt update
#
#sudo apt-get install openjdk-8-jdk
#sudo apt-get install maven
#
#java -version
#mvn -version
#
#mvn clean install

cd /hexa/be

cargo build --bin --release
