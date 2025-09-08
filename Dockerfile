# Use the official Dataflow base image
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Set environment variables for Dataflow Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="requirements.txt"

# Install system dependencies needed for your Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Copy all application files to the template directory
COPY main.py /template/
COPY metadata.json /template/

# Set the working directory
WORKDIR /template
