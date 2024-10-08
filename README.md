# Microservices Architecture for Trade Event Processing

This project implements a microservices architecture for processing trade events using Kafka for communication between services.

## Services

1. Kafka Trade Consumer
2. Event Processing Service
3. Metrics Service

## Infrastructure Components

- Kafka
- Zookeeper
- MongoDB
- Redis
- Prometheus
- Grafana

## Setup and Running

The project is split into two Docker Compose files:
- `docker-compose.infrastructure.yml`: Contains all infrastructure components
- `docker-compose.yml`: Contains the microservices

### Starting the Infrastructure

1. Create a Docker network for the project:
   ```
   docker network create microservices-network
   ```

2. Start the infrastructure components:
   ```
   docker-compose -f docker-compose.infrastructure.yml up -d
   ```

3. Wait for all infrastructure components to be fully up and running before proceeding to the next step.

### Starting the Microservices

Once the infrastructure is up and running:

1. If you're starting the application for the first time or after making changes to the code or Dockerfiles, build the images:
   ```
   docker-compose build
   ```

2. Start the microservices:
   ```
   docker-compose up -d
   ```

Note: If you haven't made any changes to the code or Dockerfiles since the last build, you can skip the build step and just run `docker-compose up -d`.

To view the logs of the microservices:
```
docker-compose logs -f
```

### Stopping the Services

1. To stop the microservices:
   ```
   docker-compose down
   ```

2. To stop the infrastructure components:
   ```
   docker-compose -f docker-compose.infrastructure.yml down
   ```

## Development

When making changes to a service:

1. Modify the code in the respective service directory.
2. Rebuild the specific service:
   ```
   docker-compose build <service-name>
   ```
3. Restart the service:
   ```
   docker-compose up -d <service-name>
   ```

## Monitoring

Access Grafana at `http://localhost:3000` to view metrics dashboards. The Prometheus web interface is available at `http://localhost:9090`.

### Verifying Metrics Collection

1. Access the Prometheus web interface at `http://localhost:9090`.
2. Go to the "Targets" page to ensure all exporters are up and being scraped successfully.
3. Use the Prometheus expression browser to query metrics:
   - For MongoDB: `mongodb_up`
   - For Kafka: `kafka_brokers`
   - For Zookeeper: `zookeeper_up`
   - For Redis: `redis_up`

4. In Grafana, create dashboards using these metrics to visualize the state and performance of your infrastructure.

## Configuration

Environment variables for each service are defined in the `docker-compose.yml` file. Modify these as needed for your environment.

## Testing

To test the system:

1. Ensure both infrastructure and services are running.
2. Produce sample trade events to the initial Kafka topic.
3. Verify that events are processed through the pipeline and stored in MongoDB.
4. Check the metrics in Grafana to monitor system performance.

## Troubleshooting

- Check the logs of individual services:
  ```
  docker-compose logs <service-name>
  ```
- For infrastructure components:
  ```
  docker-compose -f docker-compose.infrastructure.yml logs <service-name>
  ```
- Ensure all required environment variables are set correctly in `docker-compose.yml`.
- Verify that all infrastructure components are healthy using:
  ```
  docker-compose -f docker-compose.infrastructure.yml ps
  ```
- If metrics are not appearing in Grafana:
  1. Check that all exporter services are running
  2. Verify Prometheus targets are up in the Prometheus web interface
  3. Ensure Prometheus is configured as a data source in Grafana

For more detailed information about each service, refer to their respective README files in their directories.