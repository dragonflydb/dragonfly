FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install prerequisites and Latest Clang 18
RUN apt-get update && apt-get install -y \
    wget lsb-release software-properties-common gnupg \
    python3 python3-pip python3-venv \
    git build-essential cmake ninja-build jq curl \
    gcc-multilib nodejs npm thrift-compiler \
    && rm -rf /var/lib/apt/lists/*

# Run the official LLVM installation script
# This adds the apt.llvm.org repo and installs Clang
ARG LLVM_VERSION=18
RUN wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && ./llvm.sh $LLVM_VERSION all && rm llvm.sh
RUN ln -s /usr/bin/clang-$LLVM_VERSION /usr/bin/clang && \
    ln -s /usr/bin/clang++-$LLVM_VERSION /usr/bin/clang++ && \
    ln -s /usr/bin/clang-tidy-$LLVM_VERSION /usr/bin/clang-tidy

# Build and Install CodeChecker (Latest Tag)
WORKDIR /opt
RUN LATEST_TAG=$(curl -s https://api.github.com/repos/Ericsson/codechecker/releases/latest | jq -r .tag_name) && \
    git clone --branch $LATEST_TAG https://github.com/Ericsson/codechecker.git codechecker_src

# Create a virtualenv for CodeChecker to avoid system library conflicts
WORKDIR /opt/codechecker_src
RUN python3 -m venv /opt/codechecker_venv
ENV PATH="/opt/codechecker_venv/bin:$PATH"

# Install requirements and build CodeChecker package
# Let make package handle web requirements (it runs from web/ dir where
# the local tarball paths in web/requirements.txt resolve correctly)
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r analyzer/requirements.txt && \
    BUILD_LOGGER_64_BIT_ONLY=YES make package

ENV PATH="/opt/codechecker_src/build/CodeChecker/bin:$PATH"
WORKDIR /workspace
ENTRYPOINT ["CodeChecker"]
