FROM gcr.io/dataflow-templates-base/python312-template-launcher-base

# Install pipeline dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy pipeline code
COPY main.py /template/main.py

# Entry point expected by Flex Templates (do not change)
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
