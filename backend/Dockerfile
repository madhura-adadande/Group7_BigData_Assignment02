# Use Python 3.11.7 as base image
FROM python:3.11.7

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt
ENV PATH="/opt/venv/bin:$PATH"

# Copy the FastAPI application code
COPY . /app

# Expose the port FastAPI runs on
EXPOSE 8000

# Create a non-root user for security
RUN useradd -m appuser
USER appuser

# Run FastAPI app using Uvicorn
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]