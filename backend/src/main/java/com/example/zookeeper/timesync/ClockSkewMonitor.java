package com.example.zookeeper.timesync;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * Monitors clock skew across distributed system nodes.
 * Detects time drift, logs warnings, and proposes corrective actions.
 */
public class ClockSkewMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ClockSkewMonitor.class);
    
    // Clock skew thresholds
    private static final long WARN_SKEW_MS = 1000;      // 1 second
    private static final long ERROR_SKEW_MS = 5000;     // 5 seconds
    private static final long CRITICAL_SKEW_MS = 30000; // 30 seconds
    
    // ZooKeeper path for clock info
    private static final String CLOCK_INFO_PATH = "/clock-info";
    
    private final String serverId;
    private final ZooKeeper zooKeeper;
    private final ClockSynchronizer clockSync;
    private final long monitorIntervalSeconds;
    private final TimeSyncLogger fileLogger;
    
    private final ScheduledExecutorService monitorExecutor;
    private volatile ScheduledFuture<?> monitorTask;
    
    private final Map<String, PeerClockInfo> peerClocks = new ConcurrentHashMap<>();
    private volatile long maxSkewDetected = 0;
    private volatile ClockSkewSeverity currentSeverity = ClockSkewSeverity.NORMAL;
    
    public enum ClockSkewSeverity {
        NORMAL(0),
        WARNING(1),
        ERROR(2),
        CRITICAL(3);
        
        public final int level;
        ClockSkewSeverity(int level) {
            this.level = level;
        }
    }
    
    private static class PeerClockInfo {
        String peerId;
        long lastUpdateTime;
        long clockOffset;
        long skewMs;
        
        PeerClockInfo(String peerId, long clockOffset, long skewMs) {
            this.peerId = peerId;
            this.lastUpdateTime = System.currentTimeMillis();
            this.clockOffset = clockOffset;
            this.skewMs = skewMs;
        }
    }
    
    public ClockSkewMonitor(String serverId, ZooKeeper zooKeeper, 
                           ClockSynchronizer clockSync, long monitorIntervalSeconds) {
        this.serverId = serverId;
        this.zooKeeper = zooKeeper;
        this.clockSync = clockSync;
        this.monitorIntervalSeconds = Math.max(10, monitorIntervalSeconds);
        this.fileLogger = new TimeSyncLogger(serverId);
        
        this.monitorExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "ClockSkewMonitor-" + serverId);
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Start clock skew monitoring
     */
    public void start() {
        if (monitorTask != null && !monitorTask.isCancelled()) {
            logger.warn("Clock skew monitor already running for server {}", serverId);
            return;
        }
        
        // Initial publish immediately
        publishClockInfo();
        
        // Schedule periodic monitoring
        monitorTask = monitorExecutor.scheduleAtFixedRate(
            this::monitorClockSkew,
            monitorIntervalSeconds,
            monitorIntervalSeconds,
            TimeUnit.SECONDS
        );
        
        logger.info("Clock skew monitor started for server {} with interval {} seconds", 
            serverId, monitorIntervalSeconds);
        fileLogger.info("ClockSkewMonitor started with monitor interval " + monitorIntervalSeconds + " seconds");
    }
    
    /**
     * Stop clock skew monitoring
     */
    public void stop() {
        fileLogger.info("ClockSkewMonitor stopping");
        if (monitorTask != null) {
            monitorTask.cancel(false);
        }
        monitorExecutor.shutdown();
        try {
            if (!monitorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                monitorExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            monitorExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Clock skew monitor stopped for server {}", serverId);
        fileLogger.close();
    }
    
    /**
     * Publish this server's clock information to ZooKeeper
     */
    private void publishClockInfo() {
        try {
            // Ensure base path exists
            Stat stat = zooKeeper.exists(CLOCK_INFO_PATH, false);
            if (stat == null) {
                zooKeeper.create(CLOCK_INFO_PATH, new byte[0],
                    org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    org.apache.zookeeper.CreateMode.PERSISTENT);
            }
            
            // Create/update server clock info
            String clockInfoPath = CLOCK_INFO_PATH + "/" + serverId;
            Map<String, Object> clockInfo = new LinkedHashMap<>();
            clockInfo.put("serverId", serverId);
            clockInfo.put("localTime", clockSync.getCurrentTimeInstant().toEpochMilli());
            clockInfo.put("clockOffset", clockSync.getClockOffsetMillis());
            clockInfo.put("synchronized", clockSync.isSynchronized());
            clockInfo.put("timestamp", System.currentTimeMillis());
            
            String jsonData = convertToJson(clockInfo);
            byte[] data = jsonData.getBytes(StandardCharsets.UTF_8);
            
            Stat existingStat = zooKeeper.exists(clockInfoPath, false);
            if (existingStat == null) {
                zooKeeper.create(clockInfoPath, data,
                    org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    org.apache.zookeeper.CreateMode.EPHEMERAL);
            } else {
                zooKeeper.setData(clockInfoPath, data, existingStat.getVersion());
            }
            
        } catch (Exception e) {
            logger.debug("Failed to publish clock info for server {}: {}", serverId, e.getMessage());
        }
    }
    
    /**
     * Monitor clock skew across the cluster
     */
    private void monitorClockSkew() {
        try {
            publishClockInfo();
            
            List<String> peers = zooKeeper.getChildren(CLOCK_INFO_PATH, false);
            long maxSkew = 0;
            
            // Collect all peer clock offsets
            for (String peerId : peers) {
                if (peerId.equals(serverId)) continue;
                
                try {
                    String clockInfoPath = CLOCK_INFO_PATH + "/" + peerId;
                    Stat stat = zooKeeper.exists(clockInfoPath, false);
                    if (stat == null) continue;
                    
                    byte[] data = zooKeeper.getData(clockInfoPath, false, stat);
                    Map<String, Object> peerInfo = parseJson(new String(data, StandardCharsets.UTF_8));
                    
                    // Calculate skew between this server and peer
                    long peerOffset = ((Number) peerInfo.getOrDefault("clockOffset", 0)).longValue();
                    long myOffset = clockSync.getClockOffsetMillis();
                    long skew = Math.abs(peerOffset - myOffset);
                    
                    peerClocks.put(peerId, new PeerClockInfo(peerId, peerOffset, skew));
                    maxSkew = Math.max(maxSkew, skew);
                    
                } catch (Exception e) {
                    logger.debug("Failed to read clock info for peer {}: {}", peerId, e.getMessage());
                }
            }
            
            this.maxSkewDetected = maxSkew;
            updateSeverity(maxSkew);
            
            if (maxSkew > WARN_SKEW_MS) {
                logSkewWarning(maxSkew);
                fileLogger.warn("Clock skew detected: " + maxSkew + "ms from " + peers.size() + " peers");
            }
            
        } catch (Exception e) {
            logger.debug("Clock skew monitoring failed: {}", e.getMessage());
        }
    }
    
    /**
     * Update severity level based on maximum detected skew
     */
    private void updateSeverity(long skewMs) {
        ClockSkewSeverity newSeverity;
        
        if (skewMs >= CRITICAL_SKEW_MS) {
            newSeverity = ClockSkewSeverity.CRITICAL;
        } else if (skewMs >= ERROR_SKEW_MS) {
            newSeverity = ClockSkewSeverity.ERROR;
        } else if (skewMs >= WARN_SKEW_MS) {
            newSeverity = ClockSkewSeverity.WARNING;
        } else {
            newSeverity = ClockSkewSeverity.NORMAL;
        }
        
        if (newSeverity != currentSeverity) {
            logger.warn("Clock skew severity changed for server {}: {} -> {} (skew: {} ms)",
                serverId, currentSeverity, newSeverity, skewMs);
            fileLogger.warn("Severity changed: " + currentSeverity + " -> " + newSeverity + " (skew: " + skewMs + "ms)");
            currentSeverity = newSeverity;
        }
    }
    
    /**
     * Log detailed clock skew warning
     */
    private void logSkewWarning(long maxSkew) {
        StringBuilder sb = new StringBuilder();
        sb.append("Clock skew detected for server ").append(serverId).append(":\n");
        sb.append(String.format("  Max skew: %d ms\n", maxSkew));
        sb.append("  Peer details:\n");
        
        for (PeerClockInfo peer : peerClocks.values()) {
            sb.append(String.format("    %s: offset=%d ms, skew=%d ms\n", 
                peer.peerId, peer.clockOffset, peer.skewMs));
        }
        
        if (currentSeverity.level >= ClockSkewSeverity.CRITICAL.level) {
            logger.error(sb.toString());
        } else if (currentSeverity.level >= ClockSkewSeverity.ERROR.level) {
            logger.error("Clock skew critical for server {}: {} ms", serverId, maxSkew);
        } else {
            logger.warn(sb.toString());
        }
    }
    
    /**
     * Check if clock skew is too large to safely operate
     */
    public boolean isClockSkewAcceptable() {
        return maxSkewDetected < CRITICAL_SKEW_MS && clockSync.isSynchronized();
    }
    
    /**
     * Get maximum clock skew detected
     */
    public long getMaxSkewMs() {
        return maxSkewDetected;
    }
    
    /**
     * Get current severity level
     */
    public ClockSkewSeverity getSeverity() {
        return currentSeverity;
    }
    
    /**
     * Get all peer clock information
     */
    public Map<String, Object> getPeerClockInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("serverId", serverId);
        info.put("maxSkewMs", maxSkewDetected);
        info.put("severity", currentSeverity.name());
        info.put("myClockOffset", clockSync.getClockOffsetMillis());
        info.put("mySynchronized", clockSync.isSynchronized());
        
        Map<String, Object> peers = new LinkedHashMap<>();
        for (PeerClockInfo peer : peerClocks.values()) {
            Map<String, Object> peerInfo = new LinkedHashMap<>();
            peerInfo.put("offset", peer.clockOffset);
            peerInfo.put("skew", peer.skewMs);
            peers.put(peer.peerId, peerInfo);
        }
        info.put("peers", peers);
        
        return info;
    }
    
    /**
     * Simple JSON serialization (avoid dependency on external library)
     */
    private String convertToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":");
            Object val = entry.getValue();
            if (val instanceof String) {
                sb.append("\"").append(val).append("\"");
            } else {
                sb.append(val);
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * Simple JSON deserialization
     */
    private Map<String, Object> parseJson(String json) {
        Map<String, Object> map = new LinkedHashMap<>();
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) {
            return map;
        }
        
        String content = json.substring(1, json.length() - 1);
        if (content.isEmpty()) {
            return map;
        }
        
        for (String pair : content.split(",")) {
            int colonIdx = pair.indexOf(":");
            if (colonIdx > 0) {
                String key = pair.substring(0, colonIdx).trim().replace("\"", "");
                String value = pair.substring(colonIdx + 1).trim().replace("\"", "");
                
                try {
                    if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                        map.put(key, Boolean.parseBoolean(value));
                    } else {
                        map.put(key, Long.parseLong(value));
                    }
                } catch (NumberFormatException e) {
                    map.put(key, value);
                }
            }
        }
        return map;
    }
}
