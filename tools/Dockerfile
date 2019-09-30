FROM ubuntu:18.04
MAINTAINER DDlog team (https://github.com/vmware/differential-datalog)

RUN apt-get update &&   \
    apt-get install -y  \
    wget                \
    curl                \
    libc6-dev           \
    libgmp-dev          \
    default-jdk         \
    cmake               \
    subversion          \
    python              \
    python-pip          \
    autoconf            \
    libtool             \
    libssl-dev

# Install haskell stack
RUN wget -qO- https://get.haskellstack.org/ | sh

# Install FlatBuffers
COPY install-flatbuf.sh /root
RUN /root/install-flatbuf.sh

ENV PATH=/root/.local/bin:/root/.cargo/bin:$PATH
ENV CLASSPATH=$CLASSPATH:/flatbuffers/java

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain 1.34.0 -y
RUN rustup component add rustfmt
RUN rustup component add clippy

# Install Haskell dependencies to speed up builds
RUN git clone https://github.com/vmware/differential-datalog.git
RUN cd differential-datalog && stack --no-terminal --install-ghc test --only-dependencies
RUN rm -rf differential-datalog

# Python packages needed by souffle scripts
RUN pip install     \
    parglare

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
