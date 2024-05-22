# InstantDuck

## Overview

This project is a FastAPI-based application that ingests data into a Redis stream and processes it with a consumer that stores the data in a DuckDB database.
That data is then transformed using a dbt project.
The project uses Docker to containerize the application, making it easy to set up and run.

## Project Structure

```
instantduck/
├── app/
│   ├── __init__.py
│   ├── api.py
│   ├── consumer.py
│   └── requirements.txt
├── config/
│   ├── supervisord.conf
├── demo/
│   ├── demo.py
|   ├── vehicle_telemetry.json
├── rt_analytics/
├── Dockerfile
├── .gitignore
└── README.md
```

- **app/**: Contains the main application code.
  - `__init__.py`: Initializes the `app` package.
  - `api.py`: Contains the FastAPI API code.
  - `consumer.py`: Contains the consumer logic for processing messages from Redis Streams.
  - `requirements.txt`: Lists the Python dependencies required by the application.
- **config/**: Contains configuration files.
  - `supervisord.conf`: Configuration for Supervisor to manage your processes.
- **demo/**: Directory for demo scripts and data.
  - `demo.py`: Demonstrates how to ingest data into the API.
  - `vehicle_telemetry.json`: Sample data for the demo.
- **rt_analytics/**: Directory for the dbt project.
- **Dockerfile**: Defines the Docker image, including all dependencies and setup instructions.
- **.gitignore**: Specifies intentionally untracked files to ignore.
- **README.md**: Provides an overview of the project, instructions for setup, usage, and deployment.

## Setup

### Prerequisites

- Docker

### Running the Application

1. **Clone the Repository**:
   ```sh
   git clone https://github.com/aci-labs/instantduck.git
   cd instandduck
   ```

2. **Build the Docker Image**:
   ```sh
   docker build -t instantduck .
   ```

3. **Run the Docker Container**:
   ```sh
   docker run -d -p 5000:5000 --name c-instantduck instantduck
   ```

4. **Check Logs** (Optional):
   ```sh
   docker logs c-instantduck
   ```

### Testing the Application

- **Ingest Data and run dbt models**:
  ```sh
  cd demo
  python demo.py
  ```

### Directory Details

- **app/requirements.txt**: Specifies the dependencies required by the application:
  ```
  fastapi
  uvicorn
  redis
  duckdb
  dbt-duckdb
  fsspec
  ```

- **config/supervisord.conf**: Supervisor configuration to manage the processes:
  ```ini
  [supervisord]
  nodaemon=true

  [program:redis]
  command=/usr/bin/redis-server
  autorestart=true
  redirect_stderr=true

  [program:api]
  command=uvicorn app.api:app --host 0.0.0.0 --port 5000
  autorestart=true
  redirect_stderr=true

  [program:consumer]
  command=python /app/consumer.py
  autorestart=true
  redirect_stderr=true
  ```

- **Dockerfile**: Defines the Docker image:
  
- **.gitignore**: Specifies files and directories to ignore:
  ```
  
  ```

## Notes

- Ensure Redis and DuckDB are properly installed and configured in the Docker environment.
- Supervisor is used to manage the Redis server, API server, and consumer process within the Docker container.

## Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Create a new Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.