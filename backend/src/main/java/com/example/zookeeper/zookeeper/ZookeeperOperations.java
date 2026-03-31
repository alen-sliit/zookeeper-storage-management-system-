package com.example.zookeeper.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class ZookeeperOperations {
    
    private ZooKeeper zooKeeper;
    
    public ZookeeperOperations(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
    
    // 1. CREATE a node
    public void createNode(String path, String data, CreateMode mode) throws Exception {
        String createdPath = zooKeeper.create(
            path,
            data.getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,  // Open permissions (for testing)
            mode
        );
        System.out.println("Created: " + createdPath);
    }
    
    // 2. DELETE a node
    public void deleteNode(String path) throws Exception {
        // -1 means delete regardless of version
        zooKeeper.delete(path, -1);
        System.out.println("Deleted: " + path);
    }
    
    // 3. CHECK if a node exists
    public boolean exists(String path) throws Exception {
        return zooKeeper.exists(path, false) != null;
    }
    
    // 4. GET DATA from a node
    public String getData(String path) throws Exception {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }
    
    // 5. SET DATA on a node
    public void setData(String path, String data) throws Exception {
        zooKeeper.setData(path, data.getBytes(), -1);
        System.out.println("Updated " + path + " with: " + data);
    }
    
    // 6. GET CHILDREN of a node
    public List<String> getChildren(String path) throws Exception {
        return zooKeeper.getChildren(path, false);
    }
    
    // 7. CREATE EPHEMERAL node (dies when connection closes)
    public void createEphemeralNode(String path, String data) throws Exception {
        createNode(path, data, CreateMode.EPHEMERAL);
        System.out.println("Created ephemeral node: " + path);
        System.out.println("This will disappear when connection closes!");
    }
    
    // 8. CREATE SEQUENTIAL node (auto-numbered)
    public void createSequentialNode(String path, String data, boolean ephemeral) throws Exception {
        CreateMode mode;
        if (ephemeral) {
            mode = CreateMode.EPHEMERAL_SEQUENTIAL;
        } else {
            mode = CreateMode.PERSISTENT_SEQUENTIAL;
        }
        
        String createdPath = zooKeeper.create(
            path,
            data.getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            mode
        );
        System.out.println("Created sequential node: " + createdPath);
    }
    
    // 9. SET A WATCH (get notified when node changes)
    public void setWatch(String path) throws Exception {
        // Pass 'true' to set a watch
        zooKeeper.exists(path, event -> {
            System.out.println("Watch triggered on " + path);
            System.out.println("Event type: " + event.getType());
            System.out.println("Event state: " + event.getState());
        });
        System.out.println("Watch set on: " + path);
    }
    
    // 10. DEMO: Create a node and see ephemeral behavior
    public void demoEphemeralNode() throws Exception {
        String path = "/ephemeral-demo";
        
        // Create ephemeral node
        createEphemeralNode(path, "I will disappear!");
        
        // Verify it exists
        System.out.println("Does it exist? " + exists(path));
        
        // Wait for user to press Enter
        System.out.println("\nPress Enter to close connection and see ephemeral node disappear...");
        System.in.read();
    }
    
    // 11. DEMO: Sequential nodes
    public void demoSequentialNodes() throws Exception {
        System.out.println("\n=== Sequential Node Demo ===");
        createSequentialNode("/task-", "Task 1", false);
        createSequentialNode("/task-", "Task 2", false);
        createSequentialNode("/task-", "Task 3", false);
        
        List<String> children = getChildren("/");
        System.out.println("All nodes: " + children);
    }
    
    // 12. DEMO: Watch for changes
    public void demoWatch() throws Exception {
        String path = "/watch-demo";
        
        // Create the node first
        createNode(path, "initial data", CreateMode.PERSISTENT);
        
        // Set a watch
        setWatch(path);
        
        // Change the data - this will trigger the watch
        System.out.println("\nUpdating data to trigger watch...");
        setData(path, "new data");
        
        // Wait a moment for watch to trigger
        Thread.sleep(1000);
        
        // Clean up
        deleteNode(path);
    }
    
    // 13. DEMO: List all servers (simulate registration)
    public void demoServerRegistration(String serverId) throws Exception {
        String path = "/servers/" + serverId;
        
        // Register as ephemeral node
        createEphemeralNode(path, "Server " + serverId + " is alive");
        
        // List all registered servers
        List<String> servers = getChildren("/servers");
        System.out.println("Active servers: " + servers);
    }
}
