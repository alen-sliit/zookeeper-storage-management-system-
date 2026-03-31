package com.example.zookeeper.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This class handles the connection to ZooKeeper.
 * It manages the connection lifecycle and provides a connected ZooKeeper client.
 */
public class ZookeeperConnection {
    
    // ZooKeeper client instance
    private ZooKeeper zooKeeper;
    
    // Latch is used to wait for the connection to be established
    private CountDownLatch connectionLatch = new CountDownLatch(1);
    
    // Connection timeout in milliseconds
    private static final int SESSION_TIMEOUT = 3000;
    
    /**
     * Connect to ZooKeeper ensemble
     * @param connectionString Comma-separated list of ZooKeeper servers (e.g., "localhost:2181")
     * @return Connected ZooKeeper instance
     * @throws Exception If connection fails
     */
    public ZooKeeper connect(String connectionString) throws Exception {
        
        // Create watcher to listen for connection events
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // When we get a SyncConnected event, we know we're connected
                if (event.getState() == KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to ZooKeeper!");
                    connectionLatch.countDown();  // Release the latch
                } 
                // Handle disconnection
                else if (event.getState() == KeeperState.Disconnected) {
                    System.out.println("Disconnected from ZooKeeper!");
                }
                // Handle expired session
                else if (event.getState() == KeeperState.Expired) {
                    System.out.println("ZooKeeper session expired!");
                }
            }
        };
        
        // Create ZooKeeper client (this happens asynchronously)
        System.out.println("Connecting to ZooKeeper at: " + connectionString);
        this.zooKeeper = new ZooKeeper(connectionString, SESSION_TIMEOUT, watcher);
        
        // Wait for connection to complete (with timeout)
        boolean connected = connectionLatch.await(SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
        
        if (!connected) {
            throw new Exception("Could not connect to ZooKeeper within timeout");
        }
        
        return this.zooKeeper;
    }
    
    /**
     * Close the ZooKeeper connection
     */
    public void close() throws Exception {
        if (zooKeeper != null) {
            zooKeeper.close();
            System.out.println("ZooKeeper connection closed");
        }
    }
    
    /**
     * Get the ZooKeeper client instance
     */
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    
    /**
     * Check if currently connected to ZooKeeper
     */
    public boolean isConnected() {
        return zooKeeper != null && zooKeeper.getState().isConnected();
    }
}