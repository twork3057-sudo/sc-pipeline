FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

WORKDIR /template
COPY main.py .
COPY requirements.txt .

# Install deps at build time (so launch doesn’t touch PyPI)
RUN apt-get update && apt-get install -y build-essential libpq-dev pkg-config \
 && pip install --no-cache-dir -r /template/requirements.txt \
 && rm -rf /var/lib/apt/lists/*

# Tell the launcher only the entry file (NO requirements file here)
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
# (intentionally omit FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE)
