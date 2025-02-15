# Use Python 3.11.7 as base image
FROM python:3.11.7

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt
ENV PATH="/opt/venv/bin:$PATH"

# Copy the Streamlit application code
COPY app.py .

# Ensure Streamlit config directory exists
RUN mkdir -p /root/.streamlit
COPY config.toml /root/.streamlit/config.toml
RUN chmod -R 777 /root/.streamlit

# Expose the port Streamlit runs on
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]