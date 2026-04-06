package com.example.zookeeper.test;

import com.example.zookeeper.election.LeaderElection;
import com.example.zookeeper.zookeeper.ZookeeperConnection;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderElectionPerformanceTest {
    
    private static final String ZK_ADDRESS = "localhost:2181";
    private static ZookeeperConnection connectionManager;
    private static ZooKeeper zooKeeper;
    private static PerformanceMetrics metrics;
    
    @BeforeAll
    static void setUp() throws Exception {
        connectionManager = new ZookeeperConnection();
        zooKeeper = connectionManager.connect(ZK_ADDRESS);
        metrics = new PerformanceMetrics("LeaderElectionPerformance");
        
        if (zooKeeper.exists("/storage-servers", false) == null) {
            zooKeeper.create("/storage-servers", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zooKeeper.exists("/leader", false) == null) {
            zooKeeper.create("/leader", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        
        System.out.println("=== Leader Election Performance Tests ===\n");
    }
    
    @AfterAll
    static void tearDown() throws Exception {
        metrics.printSummary();
        metrics.saveToFile();
        connectionManager.close();
    }
    
    @Test
    void testElectionTimeWithMultipleServers() throws Exception {
        int[] serverCounts = {1, 3, 5, 7, 10};
        
        for (int count : serverCounts) {
            System.out.println("\n--- Testing with " + count + " servers ---");
            
            List<LeaderElection> elections = new ArrayList<>();
            List<Thread> serverThreads = new ArrayList<>();
            CountDownLatch leaderElected = new CountDownLatch(1);
            AtomicInteger leaderId = new AtomicInteger(-1);
            long startTime = System.currentTimeMillis();
            long electionTime = 0;
        
            for (int i = 1; i <= count; i++) {
                final int serverNum = i;
                String serverId = "perf-server-" + serverNum;
                LeaderElection election = new LeaderElection(zooKeeper, serverId);
                elections.add(election);
                
                Thread t = new Thread(() -> {
                    try {
                        election.initialize();
                        if (election.isLeader()) {
                            leaderId.set(serverNum);
                            leaderElected.countDown();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                serverThreads.add(t);
                t.start();
            }
                      
            boolean elected = leaderElected.await(30, TimeUnit.SECONDS);
            electionTime = System.currentTimeMillis() - startTime;
            
            metrics.recordResult(
                "Election with " + count + " servers",
                electionTime,
                elected,
                "Leader elected: server-" + leaderId.get() + " in " + electionTime + "ms"
            );
            
            for (int i = 1; i <= count; i++) {
                String serverId = "perf-server-" + i;
                try {
                    if (zooKeeper.exists("/storage-servers/" + serverId, false) != null) {
                        zooKeeper.delete("/storage-servers/" + serverId, -1);
                    }
                } catch (Exception e) {
                    // Node may already be deleted
                }
            }
            try {
                if (zooKeeper.exists("/leader/current", false) != null) {
                    zooKeeper.delete("/leader/current", -1);
                }
            } catch (Exception e) {
            }
            
            Thread.sleep(2000);
        }
    }
    
    @Test
    void testConcurrentElectionTime() throws Exception {
        int serverCount = 5;
        int testRuns = 10;
        
        System.out.println("\n--- Testing concurrent election over " + testRuns + " runs ---");
        
        List<Long> electionTimes = new ArrayList<>();
        
        for (int run = 1; run <= testRuns; run++) {
            System.out.println("  Run " + run + " of " + testRuns);
            
            List<LeaderElection> elections = new ArrayList<>();
            List<Thread> serverThreads = new ArrayList<>();
            CountDownLatch leaderElected = new CountDownLatch(1);
            long startTime = System.currentTimeMillis();
            
            for (int i = 1; i <= serverCount; i++) {
                String serverId = "concurrent-server-" + i;
                LeaderElection election = new LeaderElection(zooKeeper, serverId);
                elections.add(election);
                
                Thread t = new Thread(() -> {
                    try {
                        election.initialize();
                        if (election.isLeader()) {
                            leaderElected.countDown();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                serverThreads.add(t);
                t.start();
            }
            
            leaderElected.await(30, TimeUnit.SECONDS);
            long electionTime = System.currentTimeMillis() - startTime;
            electionTimes.add(electionTime);
            
            for (int i = 1; i <= serverCount; i++) {
                try {
                    zooKeeper.delete("/storage-servers/concurrent-server-" + i, -1);
                } catch (Exception e) {}
            }
            try {
                zooKeeper.delete("/leader/current", -1);
            } catch (Exception e) {}
            
            Thread.sleep(1000);
        }
        
        double avg = electionTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        long min = electionTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        long max = electionTimes.stream().mapToLong(Long::longValue).max().orElse(0);
        
        metrics.recordResult(
            "Concurrent election (avg over " + testRuns + " runs)",
            (long) avg,
            true,
            String.format("Min: %d ms, Max: %d ms, Avg: %.2f ms", min, max, avg)
        );
    }
}
