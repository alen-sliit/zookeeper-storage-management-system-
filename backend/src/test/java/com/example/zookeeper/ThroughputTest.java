package com.example.zookeeper.test;

import com.example.zookeeper.zookeeper.ZookeeperConnection;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests system throughput under concurrent operations.
 * For Member 4's performance evaluation.
 */
public class ThroughputTest {
    
    private static final String ZK_ADDRESS = "localhost:2181";
    private static ZookeeperConnection connectionManager;
    private static ZooKeeper zooKeeper;
    private static PerformanceMetrics metrics;
    
    @BeforeAll
    static void setUp() throws Exception {
        connectionManager = new ZookeeperConnection();
        zooKeeper = connectionManager.connect(ZK_ADDRESS);
        metrics = new PerformanceMetrics("Throughput");
        
        // Create test parent node
        if (zooKeeper.exists("/throughput-test", false) == null) {
            zooKeeper.create("/throughput-test", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        
        System.out.println("=== Throughput Tests ===\n");
    }
    
    @AfterAll
    static void tearDown() throws Exception {
        metrics.printSummary();
        metrics.saveToFile();
        connectionManager.close();
    }
    
    /**
     * Test 1: Metadata write throughput.
     */
    @Test
    void testMetadataWriteThroughput() throws Exception {
        System.out.println("\n--- Testing Metadata Write Throughput ---");
        
        int[] threadCounts = {1, 5, 10, 20, 50};
        
        for (int threads : threadCounts) {
            System.out.println("  Testing with " + threads + " concurrent writers");
            
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(threads);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);
            
            long startTime = System.currentTimeMillis();
            int operationsPerThread = 100;
            int totalOperations = threads * operationsPerThread;
            
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            String path = "/throughput-test/write-" + threadId + "-" + i;
                            try {
                                zooKeeper.create(path, 
                                    ("data-" + i).getBytes(),
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL);
                                successCount.incrementAndGet();
                            } catch (Exception e) {
                                failureCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        failureCount.addAndGet(operationsPerThread);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await(60, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startTime;
            executor.shutdown();
            
            double throughput = (totalOperations * 1000.0) / duration;
            
            metrics.recordResult(
                "Metadata write throughput with " + threads + " threads",
                duration,
                successCount.get() == totalOperations,
                String.format("Ops: %d, Success: %d, Throughput: %.2f ops/sec", 
                              totalOperations, successCount.get(), throughput)
            );
            
            // Clean up
            for (int t = 0; t < threads; t++) {
                for (int i = 0; i < operationsPerThread; i++) {
                    try {
                        zooKeeper.delete("/throughput-test/write-" + t + "-" + i, -1);
                    } catch (Exception e) {}
                }
            }
        }
    }
    
    /**
     * Test 2: Metadata read throughput.
     */
    @Test
    void testMetadataReadThroughput() throws Exception {
        System.out.println("\n--- Testing Metadata Read Throughput ---");
        
        // Create test nodes first
        int testNodeCount = 1000;
        for (int i = 0; i < testNodeCount; i++) {
            String path = "/throughput-test/read-node-" + i;
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, ("data-" + i).getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
        
        int[] threadCounts = {1, 5, 10, 20, 50};
        
        for (int threads : threadCounts) {
            System.out.println("  Testing with " + threads + " concurrent readers");
            
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(threads);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);
            
            long startTime = System.currentTimeMillis();
            int operationsPerThread = 100;
            int totalOperations = threads * operationsPerThread;
            
            for (int t = 0; t < threads; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            int nodeIndex = (int) (Math.random() * testNodeCount);
                            String path = "/throughput-test/read-node-" + nodeIndex;
                            try {
                                byte[] data = zooKeeper.getData(path, false, null);
                                if (data != null) {
                                    successCount.incrementAndGet();
                                } else {
                                    failureCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                failureCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        failureCount.addAndGet(operationsPerThread);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await(60, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startTime;
            executor.shutdown();
            
            double throughput = (totalOperations * 1000.0) / duration;
            
            metrics.recordResult(
                "Metadata read throughput with " + threads + " threads",
                duration,
                successCount.get() > 0,
                String.format("Ops: %d, Success: %d, Throughput: %.2f ops/sec", 
                              totalOperations, successCount.get(), throughput)
            );
        }
        
        // Clean up
        for (int i = 0; i < testNodeCount; i++) {
            try {
                zooKeeper.delete("/throughput-test/read-node-" + i, -1);
            } catch (Exception e) {}
        }
    }
    
    /**
     * Test 3: Concurrent create operations (for locks simulation).
     */
    @Test
    void testConcurrentCreateThroughput() throws Exception {
        System.out.println("\n--- Testing Concurrent Create Operations ---");
        
        int[] threadCounts = {1, 5, 10, 20, 50};
        
        for (int threads : threadCounts) {
            System.out.println("  Testing with " + threads + " concurrent creators");
            
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(threads);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);
            
            long startTime = System.currentTimeMillis();
            int operationsPerThread = 50;
            int totalOperations = threads * operationsPerThread;
            
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            String path = "/throughput-test/concurrent-" + threadId + "-" + i;
                            try {
                                zooKeeper.create(path, 
                                    ("data").getBytes(),
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL_SEQUENTIAL);
                                successCount.incrementAndGet();
                            } catch (Exception e) {
                                failureCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        failureCount.addAndGet(operationsPerThread);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await(60, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startTime;
            executor.shutdown();
            
            double throughput = (totalOperations * 1000.0) / duration;
            
            metrics.recordResult(
                "Concurrent create throughput with " + threads + " threads",
                duration,
                successCount.get() > 0,
                String.format("Ops: %d, Success: %d, Throughput: %.2f ops/sec", 
                              totalOperations, successCount.get(), throughput)
            );
            
            // Clean up
            List<String> children = zooKeeper.getChildren("/throughput-test", false);
            for (String child : children) {
                if (child.startsWith("concurrent-")) {
                    try {
                        zooKeeper.delete("/throughput-test/" + child, -1);
                    } catch (Exception e) {}
                }
            }
        }
    }
    
    /**
     * Test 4: Latency distribution.
     */
    @Test
    void testOperationLatency() throws Exception {
        System.out.println("\n--- Testing Operation Latency ---");
        
        int operations = 100;
        List<Long> createLatencies = new ArrayList<>();
        List<Long> readLatencies = new ArrayList<>();
        List<Long> deleteLatencies = new ArrayList<>();
        
        for (int i = 0; i < operations; i++) {
            String path = "/throughput-test/latency-test-" + i;
            
            // Create latency
            long start = System.nanoTime();
            zooKeeper.create(path, "data".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            createLatencies.add(System.nanoTime() - start);
            
            // Read latency
            start = System.nanoTime();
            zooKeeper.getData(path, false, null);
            readLatencies.add(System.nanoTime() - start);
            
            // Delete latency
            start = System.nanoTime();
            zooKeeper.delete(path, -1);
            deleteLatencies.add(System.nanoTime() - start);
        }
        
        // Calculate statistics
        long createAvg = createLatencies.stream().mapToLong(Long::longValue).sum() / operations;
        long readAvg = readLatencies.stream().mapToLong(Long::longValue).sum() / operations;
        long deleteAvg = deleteLatencies.stream().mapToLong(Long::longValue).sum() / operations;
        
        long createMax = createLatencies.stream().mapToLong(Long::longValue).max().orElse(0);
        long readMax = readLatencies.stream().mapToLong(Long::longValue).max().orElse(0);
        long deleteMax = deleteLatencies.stream().mapToLong(Long::longValue).max().orElse(0);
        
        metrics.recordResult(
            "Operation latency (create)",
            createAvg / 1000000,
            true,
            String.format("Avg: %d μs, Max: %d μs", createAvg / 1000, createMax / 1000)
        );
        
        metrics.recordResult(
            "Operation latency (read)",
            readAvg / 1000000,
            true,
            String.format("Avg: %d μs, Max: %d μs", readAvg / 1000, readMax / 1000)
        );
        
        metrics.recordResult(
            "Operation latency (delete)",
            deleteAvg / 1000000,
            true,
            String.format("Avg: %d μs, Max: %d μs", deleteAvg / 1000, deleteMax / 1000)
        );
    }
}
