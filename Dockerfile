# Use the official Python image as the base
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Set environment variables (if any)
# ENV VARIABLE_NAME=value

# Set the entrypoint
ENTRYPOINT ["python", "main.py"]