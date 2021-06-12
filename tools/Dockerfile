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
    python3             \
    python3-pip         \
    autoconf            \
    libtool             \
    libssl-dev          \
    time                \
    zookeeper           \
    libgoogle-perftools-dev \
    maven               \
    openssl             \
    gnuplot-qt          \
    pkg-config

###############################################################################
## If you modify this file don't forget to also update install-dependencies.sh

# Install haskell stack
RUN wget -qO- https://get.haskellstack.org/ | sh

# Install FlatBuffers
COPY install-flatbuf.sh /root
RUN /root/install-flatbuf.sh

ENV PATH=/root/.local/bin:/root/.cargo/bin:$PATH
ENV CLASSPATH=$CLASSPATH:/flatbuffers/java

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain 1.52.1 -y
RUN rustup component add rustfmt
RUN rustup component add clippy
RUN cargo install cargo-make

# Install Haskell dependencies to speed up builds
RUN git clone https://github.com/vmware/differential-datalog.git
RUN cd differential-datalog && stack --no-terminal --install-ghc test --only-dependencies
RUN rm -rf differential-datalog

# Python packages needed by souffle scripts
RUN python3 -m pip install parglare==0.12.0
# TODO: Delete this once the python3 support is merged
RUN pip install parglare==0.10.0

# Install Golang
RUN curl -o go.tar.gz https://dl.google.com/go/go1.13.8.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go.tar.gz
RUN rm go.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
ENV PATH=$PATH:/usr/local/go/bin
