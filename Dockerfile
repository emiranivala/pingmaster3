FROM python:3.10.4-slim-buster

# Update package lists and install required packages in one layer
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y git curl wget bash neofetch ffmpeg software-properties-common && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip3 install --upgrade pip

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install wheel && \
    pip3 install --no-cache-dir -U -r requirements.txt

# Set working directory and copy the application code
WORKDIR /app
COPY . .

# Expose port 8000 (if needed)
EXPOSE 8000

# Start both the Flask server and your crushe module.
# Note: For production, consider using a process manager like supervisord.
CMD flask run -h 0.0.0.0 -p 8000 & python3 -m crushe
