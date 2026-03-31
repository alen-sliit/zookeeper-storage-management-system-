# Backend Architecture

## Overview

This is a distributed file storage system using ZooKeeper for coordination. The system consists of multiple storage nodes that participate in leader election, handle file operations, and maintain consistency through replication.

## Key Components

### StorageNode
- Main class for each storage node
- Manages ZooKeeper connection, leader election, and file storage
- Runs as a separate process for each node

### LeaderElection
- Handles leader election using ZooKeeper ephemeral nodes
- Ensures only one leader at a time for write operations

### FileStorageManager
- Manages local file storage and replication
- Handles file chunking and metadata storage

### TimeSyncService
- Synchronizes clocks across nodes using NTP
- Monitors clock skew and handles time-related operations

### WebClientServer
- Provides a web-based UI for file operations
- Uses Java's HttpServer for REST API endpoints
- Serves static web UI files

### ConsensusMonitor
- Monitors cluster health and consensus
- Tracks server status and handles failover

## Architecture Flow

1. **Startup**: Each node connects to ZooKeeper and joins the cluster
2. **Leader Election**: Nodes elect a leader for coordinating writes
3. **File Operations**:
    - Writes go through the leader for versioning
    - Reads can be served by any available replica
4. **Replication**: Files are replicated across multiple nodes (RF=3)
5. **Failover**: If leader fails, new leader is elected automatically

## Endpoints (Web UI)

- `GET /` → Web UI interface
- `POST /api/upload` → Upload file
- `GET /api/download` → Download file
- `DELETE /api/delete` → Delete file
- `GET /api/list` → List files
- `GET /api/info` → System info
- `GET /api/stats` → Performance stats
- `GET /api/leader` → Current leader
- `GET /api/servers` → Server list

## Data Flow

Client → WebClientServer → ZooKeeper (metadata) → StorageNode → FileStorageManager → Local Storage

## Consistency Model

- Eventual consistency with leader-based writes
- Replication factor of 3
- Asynchronous replication to followers