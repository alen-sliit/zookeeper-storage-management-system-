package com.example.zookeeper.timesync;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Time Synchronization Service (Member 3 implementation)
 * 
 * Coordinates NTP synchronization, clock skew monitoring, and failure handling.
 * Provides a single interface for time queries and synchronization status.
 */
public class TimeSyncService {
    private static final Logger logger = LoggerFactory.getLogger(TimeSyncService.class);
    
    private static final long DEFAULT_SYNC_INTERVAL_SECONDS = 300; // 5 minutes
    private static final long DEFAULT_MONITOR_INTERVAL_SECONDS = 30; // 30 seconds
    
    private final String serverId;
    private final ZooKeeper zooKeeper;
    private final ClockSynchronizer clockSynchronizer;
    private final ClockSkewMonitor skewMonitor;
    private final TimeSyncLogger fileLogger;
    
    private volatile boolean running = false;
    private final Map<String, Object> syncStatistics = new ConcurrentHashMap<>();
    
    public TimeSyncService(String serverId, ZooKeeper zooKeeper) {
        this(serverId, zooKeeper, DEFAULT_SYNC_INTERVAL_SECONDS, DEFAULT_MONITOR_INTERVAL_SECONDS);
    }
    
    public TimeSyncService(String serverId, ZooKeeper zooKeeper, 
                          long syncIntervalSeconds, long monitorIntervalSeconds) {
        this.serverId = serverId;
        this.zooKeeper = zooKeeper;
        this.fileLogger = new TimeSyncLogger(serverId);
        
        this.clockSynchronizer = new ClockSynchronizer(serverId, syncIntervalSeconds);
        this.skewMonitor = new ClockSkewMonitor(serverId, zooKeeper, clockSynchronizer, monitorIntervalSeconds);
        
        fileLogger.info("TimeSyncService initialized with syncInterval=" + syncIntervalSeconds + "s, monitorInterval=" + monitorIntervalSeconds + "s");
        logger.info("TimeSyncService initialized for server {}", serverId);
    }
    
    /**
     * Start the time synchronization service
     */
    public void start() {
        if (running) {
            logger.warn("TimeSyncService already running for server {}", serverId);
            fileLogger.warn("Start called but already running");
            return;
        }
        
        try {
            clockSynchronizer.start();
            skewMonitor.start();
            running = true;
            
            logger.info("TimeSyncService started for server {}", serverId);
            fileLogger.info("TimeSyncService STARTED");
        } catch (Exception e) {
            logger.error("Failed to start TimeSyncService for server {}", serverId, e);
            fileLogger.error("Failed to start TimeSyncService", e);
            throw new RuntimeException("Failed to start time sync service", e);
        }
    }
    
    /**
     * Stop the time synchronization service
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        try {
            skewMonitor.stop();
            clockSynchronizer.stop();
            running = false;
            
            logger.info("TimeSyncService stopped for server {}", serverId);
            fileLogger.info("TimeSyncService STOPPED");
            fileLogger.close();
        } catch (Exception e) {
            logger.error("Failed to stop TimeSyncService for server {}", serverId, e);
            fileLogger.error("Error during stop", e);
        }
    }
    
    /**
     * Get current time synchronized with NTP
     */
    public long getCurrentTimeMillis() {
        return clockSynchronizer.getCurrentTimeMillis();
    }
    
    /**
     * Get current time as Instant synchronized with NTP
     */
    public Instant getCurrentTimeInstant() {
        return clockSynchronizer.getCurrentTimeInstant();
    }
    
    /**
     * Check if time synchronization is healthy
     */
    public boolean isTimeSyncHealthy() {
        return clockSynchronizer.isSynchronized() && skewMonitor.isClockSkewAcceptable();
    }
    
    /**
     * Get clock offset from NTP (debugging)
     */
    public long getClockOffsetMillis() {
        return clockSynchronizer.getClockOffsetMillis();
    }
    
    /**
     * Get maximum clock skew detected
     */
    public long getMaxClockSkewMs() {
        return skewMonitor.getMaxSkewMs();
    }
    
    /**
     * Get clock skew severity
     */
    public ClockSkewMonitor.ClockSkewSeverity getSkewSeverity() {
        return skewMonitor.getSeverity();
    }
    
    /**
     * Force immediate synchronization
     */
    public void forceSyncNow() {
        clockSynchronizer.forceSync();
        logger.info("Forced immediate clock sync for server {}", serverId);
    }
    
    /**
     * Check if skew is within acceptable bounds
     */
    public boolean isClockSkewAcceptable() {
        return skewMonitor.isClockSkewAcceptable();
    }
    
    /**
     * Get comprehensive time synchronization status
     */
    public Map<String, Object> getTimeSyncStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        
        status.put("serverId", serverId);
        status.put("running", running);
        status.put("healthy", isTimeSyncHealthy());
        
        // Clock sync status
        Map<String, Object> syncStatus = clockSynchronizer.getSyncStatus();
        status.put("synchronizer", syncStatus);
        
        // Skew monitor status
        Map<String, Object> skewStatus = skewMonitor.getPeerClockInfo();
        status.put("skewMonitor", skewStatus);
        
        // Summary
        status.put("currentTime", getCurrentTimeInstant().toString());
        status.put("skewSeverity", getSkewSeverity().name());
        status.put("acceptableSkew", isClockSkewAcceptable());
        
        return status;
    }
    
    /**
     * Handle scenario where time synchronization fails
     * 
     * Fallback strategy:
     * 1. Log critical error
     * 2. Attempt immediate sync with all NTP servers
     * 3. If still failing, use ZooKeeper server time as reference
     * 4. Alert monitoring systems
     * 5. Optionally reject write operations if skew exceeds CRITICAL threshold
     */
    public void handleSyncFailure() {
        logger.error("Time synchronization FAILURE detected for server {}", serverId);
        
        // Attempt immediate forced sync
        try {
            forceSyncNow();
            logger.info("Forced sync completed for server {}", serverId);
        } catch (Exception e) {
            logger.error("Failed to recover from sync failure for server {}", serverId, e);
        }
        
        // Check if we can still operate safely
        if (!isClockSkewAcceptable()) {
            logger.error("CRITICAL: Clock skew too large for server {} ({} ms). " +
                        "Recommend immediate administrator intervention.", 
                        serverId, getMaxClockSkewMs());
            
            // Trigger alert mechanism (would integrate with monitoring system)
            //AlertManager.alert("CRITICAL_CLOCK_SKEW", serverId, getMaxClockSkewMs());
        }
    }
    
    /**
     * Determine if write operations should be allowed
     * 
     * Write operations may need to be rejected or logged if:
     * - Clock skew exceeds CRITICAL threshold
     * - NTP synchronization has failed
     * - Skew is in ERROR severity range and growing
     */
    public boolean canPerformWriteOperation() {
        // Allow writes only if time is healthy or skew is within acceptable bounds
        if (!clockSynchronizer.isSynchronized()) {
            logger.warn("Write operation attempted with unsynchronized clock on server {}", serverId);
            fileLogger.warn("Write REJECTED: NTP not synchronized");
            return false; // Could be made optional based on policy
        }
        
        if (!isClockSkewAcceptable()) {
            logger.error("Write operation REJECTED due to critical clock skew on server {}", serverId);
            fileLogger.error("Write REJECTED: Clock skew CRITICAL (" + getMaxClockSkewMs() + "ms)", null);
            return false;
        }
        
        if (getSkewSeverity().level >= ClockSkewMonitor.ClockSkewSeverity.ERROR.level) {
            logger.warn("Write operation allowed but clock skew is in ERROR state on server {}", serverId);
            fileLogger.warn("Write allowed with WARNING: Clock skew in ERROR state (" + getMaxClockSkewMs() + "ms)");
            // Still allow but log warning
            return true;
        }
        
        return true;
    }
    
    /**
     * Determine if read operations should be allowed
     * 
     * Read operations are more tolerant of clock skew but should still warn
     */
    public boolean canPerformReadOperation() {
        // Reads are allowed even with higher skew, but log warnings
        if (getSkewSeverity().level >= ClockSkewMonitor.ClockSkewSeverity.CRITICAL.level) {
            logger.error("Read operation allowed but clock skew is CRITICAL on server {}", serverId);
        } else if (getSkewSeverity().level >= ClockSkewMonitor.ClockSkewSeverity.ERROR.level) {
            logger.warn("Read operation with ERROR-level clock skew on server {}: {} ms", 
                       serverId, getMaxClockSkewMs());
        }
        
        return true; // Reads are always allowed
    }
    
    /**
     * Get health report for operations
     */
    public String getHealthReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== TIME SYNCHRONIZATION REPORT for ").append(serverId).append(" ===\n");
        sb.append(String.format("Status: %s\n", running ? "RUNNING" : "STOPPED"));
        sb.append(String.format("Healthy: %s\n", isTimeSyncHealthy()));
        sb.append(String.format("NTP Synchronized: %s\n", clockSynchronizer.isSynchronized()));
        sb.append(String.format("Clock Offset: %d ms\n", getClockOffsetMillis()));
        sb.append(String.format("Max Skew Detected: %d ms\n", getMaxClockSkewMs()));
        sb.append(String.format("Skew Severity: %s\n", getSkewSeverity()));
        sb.append(String.format("Skew Acceptable: %s\n", isClockSkewAcceptable()));
        sb.append(String.format("Current Time: %s\n", getCurrentTimeInstant()));
        sb.append(String.format("Can Write: %s\n", canPerformWriteOperation()));
        sb.append(String.format("Can Read: %s\n", canPerformReadOperation()));
        
        return sb.toString();
    }
    
    /**
     * Get whether service is running
     */
    public boolean isRunning() {
        return running;
    }
}
