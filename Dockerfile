# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
# - gcc: Required for compiling Python packages with C extensions.
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Scrapy project into the container
COPY . .

# Set environment variables
# - PYTHONUNBUFFERED=1: Ensures Python output is sent directly to the terminal (stdout/stderr) without buffering.
ENV PYTHONUNBUFFERED=1
ENV USE_DOTENV=false

# Run the Scrapy spider
CMD ["python", "run_lsm.py"]
