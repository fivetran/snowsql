FROM openjdk:8u332-jdk-bullseye AS linux-builder
ENV DEBIAN_FRONTEND=noninteractive
ARG BAZEL_LINUX_CACHE_ARGS=--remote_cache=https://storage.googleapis.com/snowsql-bazel-linux-cache --google_credentials=service_account_credentials.json

ADD . /build
WORKDIR /build

RUN apt-get update \
  && apt-get install -y curl gnupg \
  && apt-get update \
  && apt-get install -y \
    g++ \
    make \
    nodejs \
    npm \
    python \
    python3-distutils \
    tzdata \
  && apt-get autoremove -y \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/*
RUN npx @bazel/bazelisk build --copt "-O3" --config=g++ ${BAZEL_LINUX_CACHE_ARGS} //zetasql/local_service:libremote_server.so //zetasql/local_service:remote_server_executable -- -zetasql/jdk/... -java/... -javatests/...
RUN npx @bazel/bazelisk test ${BAZEL_LINUX_CACHE_ARGS} -- //... -zetasql/jdk/... -java/... -javatests/... -zetasql/local_service:libremote_server.dylib \
  -zetasql/reference_impl:algebrizer_test \
  -zetasql/resolved_ast:resolved_ast_deep_copy_visitor_test \
  -zetasql/reference_impl:reference_compliance_test_minimal_rewrites \
  -zetasql/reference_impl:reference_compliance_test \
  -zetasql/reference_impl:reference_compliance_test_all_rewrites

FROM alpine
ARG TARGETPLATFORM
COPY --from=linux-builder /build/bazel-bin/zetasql/local_service/libremote_server.so ${TARGETPLATFORM}/libremote_server.so
COPY --from=linux-builder /build/bazel-bin/zetasql/local_service/remote_server_executable ${TARGETPLATFORM}/remote_server_executable
