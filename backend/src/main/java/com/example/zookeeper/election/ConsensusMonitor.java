package com.example.zookeeper.election;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.atomic.AtomicLong;

public class ConsensusMonitor {
    
    private final ZooKeeper zooKeeper;
    private final String serverId;
    
    
    private final AtomicLong leaderElectionCount = new AtomicLong(0);
    private final AtomicLong sessionExpirations = new AtomicLong(0);
    private final AtomicLong reconnections = new AtomicLong(0);
    private long lastLeaderElectionTime = 0;
    private long lastLeaderChangeTime = 0;
    private String currentLeader = null;
    
    public ConsensusMonitor(ZooKeeper zooKeeper, String serverId) {
        this.zooKeeper = zooKeeper;
        this.serverId = serverId;
        setupMonitor();
    }
    
    private void setupMonitor() {
        try {
            String leaderPath = "/leader/current";
            Stat stat = zooKeeper.exists(leaderPath, new LeaderChangeWatcher());
            
            if (stat != null) {
                byte[] data = zooKeeper.getData(leaderPath, false, null);
                currentLeader = new String(data);
            }
            
            System.out.println("[Monitor] Consensus monitoring active on " + serverId);
            
        } catch (Exception e) {
            System.err.println("[Monitor] Failed to setup monitoring: " + e.getMessage());
        }
    }
    
    private class LeaderChangeWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeCreated ||
                event.getType() == Event.EventType.NodeDataChanged) {
                
                try {
                    String leaderPath = "/leader/current";
                    byte[] data = zooKeeper.getData(leaderPath, false, null);
                    String newLeader = new String(data);
                    
                    if (!newLeader.equals(currentLeader)) {
                        long now = System.currentTimeMillis();
                        long downtime = lastLeaderChangeTime > 0 ? now - lastLeaderChangeTime : 0;
                        
                        System.out.println("\n[Monitor] === LEADER CHANGE DETECTED ===");
                        System.out.println("[Monitor] Old leader: " + currentLeader);
                        System.out.println("[Monitor] New leader: " + newLeader);
                        System.out.println("[Monitor] Downtime: " + downtime + " ms");
                        System.out.println("[Monitor] Election count: " + leaderElectionCount.incrementAndGet());
                        System.out.println("[Monitor] ================================\n");
                        
                        currentLeader = newLeader;
                        lastLeaderChangeTime = now;
                        lastLeaderElectionTime = now;
                    }
                    
                    zooKeeper.exists(leaderPath, this);
                    
                } catch (Exception e) {
                    System.err.println("[Monitor] Error handling leader change: " + e.getMessage());
                }
            }
        }
    }
    
    public void recordSessionExpiration() {
        long count = sessionExpirations.incrementAndGet();
        System.out.println("[Monitor] Session expired! Total expirations: " + count);
    }
    
    public void recordReconnection() {
        long count = reconnections.incrementAndGet();
        System.out.println("[Monitor] Reconnected to ZooKeeper. Total reconnections: " + count);
    }
    
    public String getHealthReport() {
        StringBuilder report = new StringBuilder();
        report.append("\n========== CONSENSUS HEALTH REPORT ==========\n");
        report.append("Server: ").append(serverId).append("\n");
        report.append("Current Leader: ").append(currentLeader).append("\n");
        report.append("Leader Elections: ").append(leaderElectionCount.get()).append("\n");
        report.append("Session Expirations: ").append(sessionExpirations.get()).append("\n");
        report.append("Reconnections: ").append(reconnections.get()).append("\n");
        
        if (lastLeaderElectionTime > 0) {
            long timeSinceLastElection = System.currentTimeMillis() - lastLeaderElectionTime;
            report.append("Time since last election: ").append(timeSinceLastElection).append(" ms\n");
        }
        
        report.append("=============================================\n");
        return report.toString();
    }
    
    public boolean isHealthy() {
        // If we've had too many elections recently, something might be wrong
        return leaderElectionCount.get() < 10;
    }

    public long getElectionCount() {
        return leaderElectionCount.get();
    }
    
  
    public void resetMetrics() {
        leaderElectionCount.set(0);
        sessionExpirations.set(0);
        reconnections.set(0);
        System.out.println("[Monitor] Metrics reset");
    }
}
