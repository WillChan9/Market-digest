FROM public.ecr.aws/docker/library/python:3.11-slim

# Install system dependencies and build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    wget \
    curl \
    unzip \
    # Python dependencies
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libsqlite3-dev \
    libreadline-dev \
    libffi-dev \
    libbz2-dev \
    # Chrome dependencies
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get update && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    rm -rf /var/lib/apt/lists/*

RUN pip install poetry

WORKDIR /app

COPY . ./

# Set PYTHONPATH so Python can find modules in /app
ENV PYTHONPATH=/app

RUN poetry lock --no-update
RUN poetry install --no-dev --no-root

# Ensure ChromeDriver has correct permissions when it's downloaded
RUN mkdir -p /root/.cache/selenium/chromedriver && \
    chmod -R 755 /root/.cache/selenium