package com.example.zookeeper.election;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * Watcher that monitors the leader node.
 * When leader disappears, this triggers a new election.
 */
public class ElectionWatcher implements Watcher {
    
    private final ZooKeeper zooKeeper;
    private final String serverId;
    private final LeaderElection leaderElection;
    private final CountDownLatch leaderElectedLatch;
    
    public ElectionWatcher(ZooKeeper zooKeeper, String serverId, 
                           LeaderElection leaderElection, 
                           CountDownLatch leaderElectedLatch) {
        this.zooKeeper = zooKeeper;
        this.serverId = serverId;
        this.leaderElection = leaderElection;
        this.leaderElectedLatch = leaderElectedLatch;
    }
    
    @Override
    public void process(WatchedEvent event) {
        String path = event.getPath();
        Watcher.Event.EventType eventType = event.getType();
        Watcher.Event.KeeperState state = event.getState();
        
        System.out.println("[Watcher] Server " + serverId + 
                          " received event: " + eventType + 
                          " on path: " + path);
        
        // Check if this is a NodeDeleted event on the leader node
        if (path != null && path.equals("/leader") && 
            eventType == Watcher.Event.EventType.NodeDeleted) {
            
            System.out.println("[Watcher] Server " + serverId + 
                              " detected leader is dead! Starting election...");
            
            // Leader is dead! Try to become the new leader
            try {
                leaderElection.electLeader();
            } catch (Exception e) {
                System.err.println("[Watcher] Election failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        // Also handle when connection is lost
        if (state == Watcher.Event.KeeperState.Disconnected) {
            System.out.println("[Watcher] Server " + serverId + 
                              " disconnected from ZooKeeper!");
        }
        
        if (state == Watcher.Event.KeeperState.SyncConnected) {
            System.out.println("[Watcher] Server " + serverId + 
                              " reconnected to ZooKeeper!");
            // Try to re-establish leadership if needed
            try {
                leaderElection.electLeader();
            } catch (Exception e) {
                System.err.println("[Watcher] Re-election failed: " + e.getMessage());
            }
        }
        
        // Signal that we've processed the event
        if (leaderElectedLatch != null) {
            leaderElectedLatch.countDown();
        }
    }
}
