# Use the official Dataflow base image
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Set environment variables for Dataflow
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="requirements.txt"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Copy all source files to the template directory
COPY . /template/

# Set working directory
WORKDIR /template

# The base image handles the entrypoint
