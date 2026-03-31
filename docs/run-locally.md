# Run Project Locally

## Prerequisites

- Java 11 or higher
- Maven 3.6+
- ZooKeeper (included in project)

## Step 1: Build the Project

From the `backend` directory:

```bash
mvn clean package
```

This compiles the code and runs tests (excluding performance tests).

## Step 2: Start ZooKeeper Cluster

From the project root:

```bash
cd zookeeper-cluster
./start.sh
```

This starts 3 ZooKeeper servers. Verify they are running by checking logs in `zookeeper-cluster/apache-zookeeper-server-*/logs/`.

## Step 3: Start Storage Nodes

From the `backend` directory:

```bash
./start.sh
```

This launches:
- 5 storage nodes (server-1 to server-5) in separate terminal windows
- 1 web UI server on port 8080

Each storage node connects to ZooKeeper at `localhost:2181,localhost:2182,localhost:2183`.

## Step 4: Access the Web UI

Open browser to: `http://localhost:8080`

The web interface provides:
- File upload/download
- File listing
- System statistics
- Leader election status
- Server information

## Step 5: Test Operations

Use the web UI or make direct HTTP requests:

- Upload: `POST http://localhost:8080/api/upload` (multipart/form-data)
- Download: `GET http://localhost:8080/api/download?filename=...`
- List: `GET http://localhost:8080/api/list`
- Delete: `DELETE http://localhost:8080/api/delete?filename=...`

## Manual Node Startup (Alternative)

If you prefer manual control, start nodes individually:

```bash
# Terminal 1 - Server 1
mvn exec:java -Dexec.args='server-1 localhost:2181,localhost:2182,localhost:2183 8081'

# Terminal 2 - Server 2
mvn exec:java -Dexec.args='server-2 localhost:2181,localhost:2182,localhost:2183 8082'

# And so on...
```

## Troubleshooting

- **ZooKeeper not starting**: Check if ports 2181-2183 are free
- **Nodes can't connect**: Ensure ZooKeeper is running and accessible
- **Web UI not loading**: Check if port 8080 is available
- **File operations fail**: Verify leader election has completed (check logs)

## Stopping the System

- Close all terminal windows (Ctrl+C in each)
- Stop ZooKeeper: `./zookeeper-cluster/apache-zookeeper-server-*/bin/zkServer.sh stop`

## Performance Testing

Run performance tests (excluded by default):

```bash
mvn test -Dtest=FailureScenarioTest,LeaderElectionPerformanceTest,ThroughputTest
```

Results are saved in `backend/test-results/`.