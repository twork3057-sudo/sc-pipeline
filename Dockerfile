# Dataflow Flex Template launcher for Python
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

WORKDIR /template

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY . .

# Tell the launcher which py file to run
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="main.py"
# (optional) if you use setup.py, point to it:
# ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="setup.py"

# Entrypoint is provided by the base image
# (/opt/google/dataflow/python_template_launcher)

