# Project Documentation

This repository contains backend logic and Zookeeper integration for a distributed file storage system.

## Modules
- Backend Service (Distributed storage nodes)
- Zookeeper Cluster (Coordination and leader election)
- Time Synchronization (NTP-based clock sync)

## Documentation Files
- `zookeeper-connection.md` → Zookeeper cluster setup & connection details
- `backend-architecture.md` → System architecture and components
- `dependencies.md` → Required libraries and versions
- `run-locally.md` → Step-by-step local setup and testing
- `time-synchronization.md` → Time sync implementation across nodes

## Purpose
This module implements a distributed file management system using ZooKeeper for coordination, featuring leader election, replication (RF=3), and eventual consistency. The system handles file upload/download operations across multiple storage nodes with automatic failover and time synchronization.

## Key Features
- Distributed file storage with replication
- Leader election and consensus
- Time synchronization across nodes
- Web-based UI for file operations
- Fault tolerance and failover handling