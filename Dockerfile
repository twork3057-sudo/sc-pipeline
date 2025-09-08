# Python Flex Template base
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Everything under /template
WORKDIR /template

# Copy your app files
COPY main.py .
COPY requirements.txt .      # <-- making sure this lands in /template

# Tell the launcher exactly where the files are
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements.txt

# Optional: system libs if need to compile wheels (psycopg2 etc.)
RUN apt-get update && apt-get install -y \
    build-essential libpq-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*

# (Optional) preinstall deps; not required for Flex Templates
# RUN pip install --no-cache-dir -r /template/requirements.txt
