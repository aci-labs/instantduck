FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    REDIS_HOST=localhost \
    REDIS_PORT=6379 

# Install Redis and supervisor
RUN apt-get update && apt-get install -y \
    redis-server \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /data 

# Set up working directory
WORKDIR /app


# Copy configuration files and Python scripts
COPY config/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY app/ .

# Expose the port for the API
EXPOSE 5000

# Start supervisor
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
