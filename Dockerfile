FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

WORKDIR /template

# Copy your app files
COPY main.py .
# Copy requirements into /template (no inline comments on COPY!)
COPY requirements.txt .

# Tell the Flex launcher exactly where things are
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements.txt

# Optional system libs
RUN apt-get update && apt-get install -y \
    build-essential libpq-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*
