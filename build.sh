#!/bin/bash
set -e

# Create output directory
mkdir -p src/proto

# Generate Rust code from protobuf
protoc \
    --rust_out=src/proto \
    --grpc_out=src/proto \
    --plugin=protoc-gen-grpc=`which grpc_rust_plugin` \
    -I proto \
    proto/k8s.io/cri-api/pkg/apis/runtime/v1alpha2/api.proto

# Format the generated code
cargo fmt --all
