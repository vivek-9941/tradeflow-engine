FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake curl git ninja-build pkg-config tar unzip zip \
    && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics
WORKDIR /workspace
COPY . .
RUN cmake -S matching-engine-cpp -B /workspace/build/cpp \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    -DVCPKG_TARGET_TRIPLET=x64-linux -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /workspace/build/cpp --target matching_engine -j2

FROM ubuntu:24.04
COPY --from=build /workspace/build/cpp/matching_engine /usr/local/bin/matching_engine
COPY --from=build /opt/vcpkg/installed/x64-linux /opt/vcpkg/installed/x64-linux
ENV LD_LIBRARY_PATH=/opt/vcpkg/installed/x64-linux/lib
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/matching_engine"]
