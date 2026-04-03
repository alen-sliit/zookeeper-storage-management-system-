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
    
    public void createNode(String path, String data, CreateMode mode) throws Exception {
        String createdPath = zooKeeper.create(
            path,
            data.getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,  
            mode
        );
        System.out.println("Created: " + createdPath);
    }
    
    public void deleteNode(String path) throws Exception {
        zooKeeper.delete(path, -1);
        System.out.println("Deleted: " + path);
    }
    
    public boolean exists(String path) throws Exception {
        return zooKeeper.exists(path, false) != null;
    }
    
    public String getData(String path) throws Exception {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }
    
    public void setData(String path, String data) throws Exception {
        zooKeeper.setData(path, data.getBytes(), -1);
        System.out.println("Updated " + path + " with: " + data);
    }
    
    public List<String> getChildren(String path) throws Exception {
        return zooKeeper.getChildren(path, false);
    }
    
    public void createEphemeralNode(String path, String data) throws Exception {
        createNode(path, data, CreateMode.EPHEMERAL);
        System.out.println("Created ephemeral node: " + path);
        System.out.println("This will disappear when connection closes!");
    }
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
    
    public void setWatch(String path) throws Exception {
      
        zooKeeper.exists(path, event -> {
            System.out.println("Watch triggered on " + path);
            System.out.println("Event type: " + event.getType());
            System.out.println("Event state: " + event.getState());
        });
        System.out.println("Watch set on: " + path);
    }
    
    
    public void demoEphemeralNode() throws Exception {
        String path = "/ephemeral-demo";
        
       
        createEphemeralNode(path, "I will disappear!");
        
      
        System.out.println("Does it exist? " + exists(path));
        
      
        System.out.println("\nPress Enter to close connection and see ephemeral node disappear...");
        System.in.read();
    }
    
    public void demoSequentialNodes() throws Exception {
        System.out.println("\n=== Sequential Node Demo ===");
        createSequentialNode("/task-", "Task 1", false);
        createSequentialNode("/task-", "Task 2", false);
        createSequentialNode("/task-", "Task 3", false);
        
        List<String> children = getChildren("/");
        System.out.println("All nodes: " + children);
    }
    public void demoWatch() throws Exception {
        String path = "/watch-demo";
        createNode(path, "initial data", CreateMode.PERSISTENT);
        
        setWatch(path);
        
        System.out.println("\nUpdating data to trigger watch...");
        setData(path, "new data");
        
        Thread.sleep(1000);
        
        deleteNode(path);
    }
    
    public void demoServerRegistration(String serverId) throws Exception {
        String path = "/servers/" + serverId;
        
        createEphemeralNode(path, "Server " + serverId + " is alive");
        List<String> servers = getChildren("/servers");
        System.out.println("Active servers: " + servers);
    }
}
