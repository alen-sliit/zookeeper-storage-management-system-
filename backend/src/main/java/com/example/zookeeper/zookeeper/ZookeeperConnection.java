package com.example.zookeeper.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZookeeperConnection {
    
    private ZooKeeper zooKeeper;
      
    private CountDownLatch connectionLatch = new CountDownLatch(1);
    
    private static final int SESSION_TIMEOUT = 3000;
    
    public ZooKeeper connect(String connectionString) throws Exception {
        
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to ZooKeeper!");
                    connectionLatch.countDown();  // Release the latch
                } 
                else if (event.getState() == KeeperState.Disconnected) {
                    System.out.println("Disconnected from ZooKeeper!");
                }
                else if (event.getState() == KeeperState.Expired) {
                    System.out.println("ZooKeeper session expired!");
                }
            }
        };
        
        System.out.println("Connecting to ZooKeeper at: " + connectionString);
        this.zooKeeper = new ZooKeeper(connectionString, SESSION_TIMEOUT, watcher);
    
        boolean connected = connectionLatch.await(SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
        
        if (!connected) {
            throw new Exception("Could not connect to ZooKeeper within timeout");
        }
        
        return this.zooKeeper;
    }
    
    public void close() throws Exception {
        if (zooKeeper != null) {
            zooKeeper.close();
            System.out.println("ZooKeeper connection closed");
        }
    }
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    
    public boolean isConnected() {
        return zooKeeper != null && zooKeeper.getState().isConnected();
    }
}
