# ZooKeeper Connection

## ZooKeeper Cluster Setup

The system uses a 3-node ZooKeeper ensemble for coordination.

### Starting ZooKeeper Cluster

Navigate to the `zookeeper-cluster` directory and run:

```bash
./start.sh
```

This starts three ZooKeeper servers on ports 2181, 2182, and 2183.

### ZooKeeper Configuration

Each server uses its own configuration file:
- `apache-zookeeper-server-1/conf/zoo.cfg`
- `apache-zookeeper-server-2/conf/zoo.cfg`
- `apache-zookeeper-server-3/conf/zoo.cfg`

Key configuration:
- `clientPort`: 2181, 2182, 2183 respectively
- `dataDir`: Points to respective data directories
- `server.1`, `server.2`, `server.3`: Ensemble configuration

### Connection String

The backend connects using: `localhost:2181,localhost:2182,localhost:2183`

### ZooKeeper Usage in System

- **Leader Election**: `/election` path for ephemeral sequential nodes
- **Server Registration**: `/servers/<serverId>` for node registration
- **File Metadata**: `/files/<fileId>` for storing file information
- **Clock Info**: `/clock-info/<serverId>` for time synchronization
- **Consensus Data**: Various paths for cluster coordination

### Client Configuration

- Session timeout: Configurable (default ~30 seconds)
- Connection retry: Automatic with exponential backoff
- Watcher events: Used for leader changes and server status updates