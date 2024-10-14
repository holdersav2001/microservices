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

The project can be run using Docker Compose or Minikube for Kubernetes deployment.

### Docker Compose Setup

To run the services using Docker Compose:

1. Start the infrastructure components:
   ```
   docker-compose -f docker-compose.infrastructure.yml up -d
   ```

2. Start the microservices:
   ```
   docker-compose up -d
   ```

To stop the services:
1. Stop the microservices:
   ```
   docker-compose down
   ```
2. Stop the infrastructure components:
   ```
   docker-compose -f docker-compose.infrastructure.yml down
   ```

### Minikube Setup

To run the services using Minikube, follow these steps:

1. Install Minikube and kubectl if you haven't already:
   - [Minikube Installation Guide](https://minikube.sigs.k8s.io/docs/start/)
   - [kubectl Installation Guide](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

2. Start Minikube:
   ```
   minikube start
   ```

3. Enable the Ingress addon:
   ```
   minikube addons enable ingress
   ```

4. Get the Minikube IP:
   ```
   minikube ip
   ```

5. Update the `kubernetes/kafka-deployment.yaml` file:
   Replace `MINIKUBE_IP` in the `KAFKA_ADVERTISED_LISTENERS` environment variable with the Minikube IP you got in step 4.

6. Set up your terminal to use Minikube's Docker daemon:

   For PowerShell:
   ```powershell
   & minikube -p minikube docker-env --shell powershell | Invoke-Expression
   ```

   For Bash (Git Bash or WSL):
   ```bash
   eval $(minikube docker-env)
   ```

7. Build Docker images for your services:
   ```
   docker build -t event-processing-service:v10 ./event_processing_service
   docker build -t kafka-trade-consumer:v2 ./kafka_trade_consumer
   docker build -t metrics-service:latest ./metrics_service
   ```

8. Verify that the images are built and available in Minikube's Docker environment:
   ```
   docker images
   ```
   You should see your service images listed.

9. Apply the updated Kubernetes manifests:
   ```
   kubectl apply -f kubernetes/
   ```

10. If you encounter any issues with the metrics-service, you can use the provided script to rebuild it:
    ```
    chmod +x build_metrics_service.sh
    ./build_metrics_service.sh
    ```

11. Wait for all pods to be in the "Running" state:
    ```
    kubectl get pods --watch
    ```

12. Access the services:
    - Kafka: `<minikube-ip>:30092`
    - MongoDB: Use port-forwarding or NodePort as defined in your MongoDB service
    - Prometheus: Use port-forwarding or NodePort as defined in your Prometheus service
    - Grafana: Use port-forwarding or NodePort as defined in your Grafana service

    To set up port-forwarding:
    ```
    kubectl port-forward service/<service-name> <local-port>:<service-port>
    ```

13. To stop and delete the Minikube cluster:
    ```
    minikube stop
    minikube delete
    ```

## Kubernetes Deployment

After successfully deploying the microservices architecture to Kubernetes, you should see the following pods running:

1. event-processing-service: Processes trade events consumed from Kafka.
2. kafka: Handles message queuing and event streaming.
3. kafka-trade-consumer: Consumes trade events from Kafka and performs initial processing.
4. metrics-service: Collects and processes metrics from the system.
5. mongodb: Stores processed trade events and other persistent data.
6. zookeeper: Manages the Kafka cluster configuration.

You can check the status of these pods using the following command:

```
kubectl get pods
```

If you see any pods in a state other than "Running", you may need to troubleshoot those specific services. Refer to the Troubleshooting section in this README for common issues and their solutions.

## Development

When making changes to a service:

For Docker Compose:
1. Modify the code in the respective service directory.
2. Rebuild the specific service:
   ```
   docker-compose build <service-name>
   ```
3. Restart the service:
   ```
   docker-compose up -d <service-name>
   ```

For Minikube:
1. Modify the code in the respective service directory.
2. Rebuild the Docker image:
   
   For PowerShell:
   ```powershell
   & minikube -p minikube docker-env --shell powershell | Invoke-Expression
   docker build -t <service-name>:latest ./<service-directory>
   ```

   For Bash (Git Bash or WSL):
   ```bash
   eval $(minikube docker-env)
   docker build -t <service-name>:latest ./<service-directory>
   ```

3. Update the Kubernetes deployment:
   ```
   kubectl rollout restart deployment/<service-name>
   ```

## Monitoring

Access Grafana and Prometheus using the NodePort or port-forwarding as set up in your Minikube environment.

### Verifying Metrics Collection

1. Access the Prometheus web interface.
2. Go to the "Targets" page to ensure all exporters are up and being scraped successfully.
3. Use the Prometheus expression browser to query metrics:
   - For MongoDB: `mongodb_up`
   - For Kafka: `kafka_brokers`
   - For Zookeeper: `zookeeper_up`
   - For Redis: `redis_up`

4. In Grafana, create dashboards using these metrics to visualize the state and performance of your infrastructure.

## Configuration

For Docker Compose:
Environment variables for each service are defined in the `docker-compose.yml` file. Modify these as needed for your environment.

For Minikube:
Update the environment variables in the respective deployment YAML files in the `kubernetes` directory.

## Testing

To test the system:

1. Ensure all services are running (either in Docker Compose or Minikube).
2. Produce sample trade events to the initial Kafka topic:
   ```
   kubectl run kafka-producer --rm -it --image=wurstmeister/kafka --command -- /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic trade_events
   ```
   Then enter sample JSON events.
3. Verify that events are processed through the pipeline:
   - Check the logs of the Kafka Trade Consumer service
   - Verify data is stored in MongoDB (use port-forwarding to access MongoDB)
4. Check the metrics in Grafana to monitor system performance.

## Troubleshooting

### Kafka Service Configuration Issue

If you encounter the following error when applying the Kubernetes configurations:

```
The Service "kafka" is invalid: spec.clusterIPs[0]: Invalid value: "None": may not be set to 'None' for NodePort services
```

Follow these steps to resolve the issue:

1. Delete the existing Kafka service:
   ```
   kubectl delete service kafka
   ```

2. Apply only the Kafka service configuration:
   ```
   kubectl apply -f kubernetes/kafka-service.yaml
   ```

3. If the above step is successful, apply the rest of the configurations:
   ```
   kubectl apply -f kubernetes/
   ```

If you still encounter issues, please check the following:

1. Ensure that the kafka-service.yaml file is correctly configured as a NodePort service.
2. Verify that there are no conflicting Kafka service definitions in other YAML files.
3. If problems persist, you may need to delete the Minikube cluster and start fresh:
   ```
   minikube delete
   minikube start
   ```
   Then follow the setup steps again from the beginning.

### Image Pull Error

If you encounter an error like this:

```
Container image "event-processing-service:latest" is not present with pull policy of Never
Error: ErrImageNeverPull
```

This indicates that the Docker image is not available in the Minikube environment. Follow these steps to resolve the issue:

1. Ensure you're using Minikube's Docker daemon:
   
   For PowerShell:
   ```powershell
   & minikube -p minikube docker-env --shell powershell | Invoke-Expression
   ```

   For Bash (Git Bash or WSL):
   ```bash
   eval $(minikube docker-env)
   ```

2. Rebuild the Docker image:
   ```
   docker build -t event-processing-service:v10 ./event_processing_service
   ```

3. Verify the image is built:
   ```
   docker images | grep event-processing-service
   ```

4. If the image is present, delete the existing deployment and reapply:
   ```
   kubectl delete deployment event-processing-service
   kubectl apply -f kubernetes/event-processing-service-deployment.yaml
   ```

5. If the issue persists, check the image pull policy in the deployment YAML file. It should be set to "Never" for local images:
   ```yaml
   imagePullPolicy: Never
   ```

6. If you've made changes to the deployment YAML, reapply it:
   ```
   kubectl apply -f kubernetes/event-processing-service-deployment.yaml
   ```

If you continue to face issues, you may need to delete the Minikube cluster and start fresh:
```
minikube delete
minikube start
```
Then follow the setup steps again from the beginning.

For more detailed information about each service, refer to their respective README files in their directories.
