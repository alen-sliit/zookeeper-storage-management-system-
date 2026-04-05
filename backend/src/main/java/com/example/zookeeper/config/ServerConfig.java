package com.example.zookeeper.config;

/**
 * Configuration class for each storage node.
 * Each server gets its own ID and knows where ZooKeeper is.
 */
public class ServerConfig {
    
    private String serverId;           // Unique ID for this server (e.g., "server-1")
    private String zookeeperAddress;   // ZooKeeper connection string
    private int storagePort;           // Port for file transfers (optional for now)
    private String storagePath;        // Local folder to store files
    
    public ServerConfig(String serverId, String zookeeperAddress, int storagePort) {
        this.serverId = serverId;
        this.zookeeperAddress = zookeeperAddress;
        this.storagePort = storagePort;
        this.storagePath = "./storage";
    }
    
    // Getters
    public String getServerId() { return serverId; }
    public String getZookeeperAddress() { return zookeeperAddress; }
    public int getStoragePort() { return storagePort; }
    public String getStoragePath() { return storagePath; }
    
    // Setters for flexibility
    public void setStoragePath(String path) { this.storagePath = path; }
    
    @Override
    public String toString() {
        return "ServerConfig{" +
                "serverId='" + serverId + '\'' +
                ", zookeeperAddress='" + zookeeperAddress + '\'' +
                ", storagePort=" + storagePort +
                ", storagePath='" + storagePath + '\'' +
                '}';
    }
}