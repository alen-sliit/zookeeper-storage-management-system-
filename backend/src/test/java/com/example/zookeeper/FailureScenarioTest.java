package com.example.zookeeper.test;

import com.example.zookeeper.election.LeaderElection;
import com.example.zookeeper.zookeeper.ZookeeperConnection;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

public class FailureScenarioTest {
    
    private static final String ZK_ADDRESS = "localhost:2181";
    private static ZookeeperConnection connectionManager;
    private static ZooKeeper zooKeeper;
    private static PerformanceMetrics metrics;
    
    @BeforeAll
    static void setUp() throws Exception {
        connectionManager = new ZookeeperConnection();
        zooKeeper = connectionManager.connect(ZK_ADDRESS);
        metrics = new PerformanceMetrics("FailureScenario");
        
        System.out.println("=== Failure Scenario Tests ===\n");
    }
    
    @AfterAll
    static void tearDown() throws Exception {
        metrics.printSummary();
        metrics.saveToFile();
        connectionManager.close();
    }
    @Test
    void testLeaderFailoverTime() throws Exception {
        System.out.println("\n--- Testing Leader Failover Time ---");
        
        // Create 3 servers
        LeaderElection election1 = new LeaderElection(zooKeeper, "failover-server-1");
        LeaderElection election2 = new LeaderElection(zooKeeper, "failover-server-2");
        LeaderElection election3 = new LeaderElection(zooKeeper, "failover-server-3");
        
        election1.initialize();
        election2.initialize();
        election3.initialize();
        
        Thread.sleep(2000);
        
        String initialLeader = getCurrentLeader();
        System.out.println("Initial leader: " + initialLeader);
        
        LeaderElection leaderElection = null;
        if (election1.isLeader()) leaderElection = election1;
        else if (election2.isLeader()) leaderElection = election2;
        else if (election3.isLeader()) leaderElection = election3;
        
        if (leaderElection != null) {
            String leaderId = leaderElection.getCurrentLeader();
            System.out.println("Killing leader: " + leaderId);
            
            long startTime = System.currentTimeMillis();
            
            safeDelete("/storage-servers/" + leaderId);
            safeDelete("/leader/current");
            
            long failoverTime = 0;
            for (int i = 0; i < 30; i++) {
                Thread.sleep(500);
                String newLeader = getCurrentLeader();
                if (newLeader != null && !newLeader.equals(leaderId)) {
                    failoverTime = System.currentTimeMillis() - startTime;
                    System.out.println("New leader elected: " + newLeader + " after " + failoverTime + "ms");
                    break;
                }
            }
            
            metrics.recordResult(
                "Leader failover",
                failoverTime,
                failoverTime > 0,
                "Failed leader: " + leaderId + ", failover in " + failoverTime + "ms"
            );
        }
        
        cleanupServers("failover-server-1", "failover-server-2", "failover-server-3");
    }

    @Test
    void testMultipleLeaderFailures() throws Exception {
        System.out.println("\n--- Testing Multiple Leader Failures ---");
        
        LeaderElection[] elections = new LeaderElection[5];
        for (int i = 0; i < 5; i++) {
            elections[i] = new LeaderElection(zooKeeper, "multi-fail-server-" + (i + 1));
            elections[i].initialize();
        }
        
        Thread.sleep(3000);
        
        List<Long> failoverTimes = new ArrayList<>();
        
        for (int failure = 1; failure <= 3; failure++) {
            String currentLeader = getCurrentLeader();
            if (currentLeader == null) break;
            
            System.out.println("Failure " + failure + ": Killing leader " + currentLeader);
            
            long startTime = System.currentTimeMillis();

            safeDelete("/storage-servers/" + currentLeader);
            safeDelete("/leader/current");
            long failoverTime = 0;
            for (int i = 0; i < 30; i++) {
                Thread.sleep(500);
                String newLeader = getCurrentLeader();
                if (newLeader != null && !newLeader.equals(currentLeader)) {
                    failoverTime = System.currentTimeMillis() - startTime;
                    System.out.println("  New leader: " + newLeader + " after " + failoverTime + "ms");
                    failoverTimes.add(failoverTime);
                    break;
                }
            }
            
            Thread.sleep(2000);
        }
        
        double avgFailover = failoverTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        metrics.recordResult(
            "Multiple leader failures (3 failures)",
            (long) avgFailover,
            failoverTimes.size() == 3,
            "Average failover time: " + avgFailover + "ms"
        );
        
       
        for (int i = 0; i < 5; i++) {
            safeDelete("/storage-servers/multi-fail-server-" + (i + 1));
        }
        safeDelete("/leader/current");
    }
    
    @Test
    void testNetworkPartition() throws Exception {
        System.out.println("\n--- Testing Network Partition (Simulated) ---");
        
        LeaderElection election1 = new LeaderElection(zooKeeper, "partition-server-1");
        LeaderElection election2 = new LeaderElection(zooKeeper, "partition-server-2");
        LeaderElection election3 = new LeaderElection(zooKeeper, "partition-server-3");
        
        election1.initialize();
        election2.initialize();
        election3.initialize();
        
        Thread.sleep(3000);
        
        String initialLeader = getCurrentLeader();
        System.out.println("Initial leader: " + initialLeader);
        
        System.out.println("Simulating network partition - isolating follower");
        
        String follower = null;
        for (int i = 1; i <= 3; i++) {
            String id = "partition-server-" + i;
            if (!id.equals(initialLeader)) {
                follower = id;
                break;
            }
        }
        
        if (follower != null) {
            long startTime = System.currentTimeMillis();
            
            safeDelete("/storage-servers/" + follower);
            
            long partitionTime = System.currentTimeMillis() - startTime;
            System.out.println("Follower " + follower + " isolated after " + partitionTime + "ms");
            
            String currentLeader = getCurrentLeader();
            boolean systemHealthy = currentLeader != null && currentLeader.equals(initialLeader);
            
            metrics.recordResult(
                "Network partition (follower isolated)",
                partitionTime,
                systemHealthy,
                "Isolated: " + follower + ", Leader still: " + currentLeader
            );
        }
        
        cleanupServers("partition-server-1", "partition-server-2", "partition-server-3");
    }
    
    private String getCurrentLeader() throws Exception {
        try {
            byte[] data = zooKeeper.getData("/leader/current", false, null);
            return new String(data);
        } catch (Exception e) {
            return null;
        }
    }
    private void cleanupServers(String... serverIds) throws Exception {
        for (String id : serverIds) {
            safeDelete("/storage-servers/" + id);
        }
        safeDelete("/leader/current");
    }

    private void safeDelete(String path) {
        try {
            zooKeeper.delete(path, -1);
        } catch (KeeperException.NoNodeException ignored) {
        } catch (Exception ignored) {
        }
    }
}
