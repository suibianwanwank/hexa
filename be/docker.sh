#!/bin/bash

# protoc env
wget https://github.com/protocolbuffers/protobuf/releases/download/v26.1/protoc-26.1-linux-x86_64.zip

unzip protoc-26.1-linux-x86_64.zip -d protoc

export PATH="$PWD/protoc/bin:$PATH"

protoc --version

cd be

cargo build --bin hexa-be --release
