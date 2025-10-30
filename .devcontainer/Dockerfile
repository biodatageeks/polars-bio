FROM mcr.microsoft.com/devcontainers/rust:2-1-bookworm

ARG CLAUDE_CODE_VERSION=latest
ARG PYTHON_VERSION=3.13.9

USER root

# Install system dependencies for bioinformatics libraries and Python build tools
RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    build-essential \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libcurl4-openssl-dev \
    libffi-dev \
    libsqlite3-dev \
    libreadline-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python 3.13.9 from source
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz && \
    tar -xf Python-${PYTHON_VERSION}.tar.xz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && \
    rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tar.xz && \
    ln -sf /usr/local/bin/python3.13 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip3.13 /usr/local/bin/pip3

# Install Node.js 24.x (latest LTS) from NodeSource
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_24.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Claude Code and OpenSpec
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}
RUN npm install -g @fission-ai/openspec@latest

# Install Poetry for Python dependency management
RUN pip3 install --no-cache-dir poetry==1.8.5

# Install Maturin for Rust-Python bindings
RUN pip3 install --no-cache-dir "maturin>=1.0,<2.0"

# Pre-install common Rust dependencies to speed up builds
RUN cargo install cargo-edit --locked

USER vscode

# Configure Poetry to create virtual environments in project directory
RUN poetry config virtualenvs.in-project true

# Set up Rust environment for user
ENV PATH="/home/vscode/.cargo/bin:${PATH}"