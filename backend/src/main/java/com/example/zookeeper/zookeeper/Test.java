package com.example.zookeeper.zookeeper;

import org.apache.zookeeper.ZooKeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.List;

/**
 * Main class to test the ZooKeeper connection and basic operations.
 * This is where you'll test that everything is working.
 */
public class Test {
    
    public static void main(String[] args) {
        
        // Create connection manager
        ZookeeperConnection connectionManager = new ZookeeperConnection();
        ZooKeeper zooKeeper = null;
        
        try {
            // Step 1: Connect to ZooKeeper
            // If you're running ZooKeeper locally on default port
            String connectionString = "localhost:2181";
            
            // If you have multiple ZooKeeper servers locally:
            // String connectionString = "localhost:2181,localhost:2182,localhost:2183";
            
            System.out.println("=== Starting ZooKeeper Connection Test ===");
            zooKeeper = connectionManager.connect(connectionString);
            
            // Step 2: Show connection info
            System.out.println("\n=== Connection Information ===");
            System.out.println("Session ID: " + zooKeeper.getSessionId());
            System.out.println("Connection State: " + zooKeeper.getState());
            
            // Step 3: Test creating a node
            System.out.println("\n=== Testing Node Creation ===");
            String testPath = "/test-node";
            
            // Check if node exists
            if (zooKeeper.exists(testPath, false) == null) {
                // Create a persistent node
                String createdPath = zooKeeper.create(
                    testPath,
                    "Hello from distributed file system!".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
                );
                System.out.println("Created node: " + createdPath);
            } else {
                System.out.println("Node " + testPath + " already exists");
            }
            
            // Step 4: Read data from the node
            System.out.println("\n=== Reading Node Data ===");
            byte[] data = zooKeeper.getData(testPath, false, null);
            String dataString = new String(data);
            System.out.println("Data at " + testPath + ": " + dataString);
            
            // Step 5: List children of root
            System.out.println("\n=== Listing Root Nodes ===");
            List<String> children = zooKeeper.getChildren("/", false);
            System.out.println("Nodes under / : " + children);
            
            // Step 6: Clean up (optional - delete the test node)
            System.out.println("\n=== Cleaning Up ===");
            zooKeeper.delete(testPath, -1);
            System.out.println("Deleted test node: " + testPath);
            
            System.out.println("\n=== All Tests Passed! ===");
            
        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Always close the connection
            try {
                connectionManager.close();
            } catch (Exception e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }
    }
}
