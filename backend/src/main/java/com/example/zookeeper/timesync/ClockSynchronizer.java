package com.example.zookeeper.timesync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * NTP-based clock synchronizer for distributed systems.
 * Syncs the system clock against public NTP servers and tracks clock offset.
 */
public class ClockSynchronizer {
    private static final Logger logger = LoggerFactory.getLogger(ClockSynchronizer.class);
    private final TimeSyncLogger fileLogger;
    
    // NTP Pool servers (maintained by volunteers for high accuracy)
    private static final String[] NTP_SERVERS = {
        "pool.ntp.org",
        "time.nist.gov",
        "time.cloudflare.com"
    };
    
    // NTP Protocol Constants
    private static final int NTP_PACKET_SIZE = 48;
    private static final int NTP_PORT = 123;
    private static final int NTP_VERSION = 3;
    private static final long NTP_EPOCH_OFFSET = 2208988800L; // Seconds between 1900 and 1970
    private static final int TIMEOUT_MS = 5000;
    
    private final String serverId;
    private volatile long clockOffset = 0; // Offset from NTP in milliseconds
    private volatile long lastSyncTime = 0;
    private volatile boolean synchronized_flag = false;
    private volatile Exception lastSyncError = null;
    
    private final ScheduledExecutorService syncExecutor;
    private volatile ScheduledFuture<?> syncTask;
    private final long syncIntervalSeconds;
    
    public ClockSynchronizer(String serverId, long syncIntervalSeconds) {
        this.serverId = serverId;
        this.syncIntervalSeconds = Math.max(60, syncIntervalSeconds); // Minimum 60 seconds
        this.fileLogger = new TimeSyncLogger(serverId);
        this.syncExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "ClockSync-" + serverId);
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Start periodic clock synchronization
     */
    public void start() {
        if (syncTask != null && !syncTask.isCancelled()) {
            logger.warn("Clock synchronizer already running for server {}", serverId);
            return;
        }
        
        // Initial sync immediately
        syncWithNTP();
        
        // Schedule periodic syncs
        syncTask = syncExecutor.scheduleAtFixedRate(
            this::syncWithNTP,
            syncIntervalSeconds,
            syncIntervalSeconds,
            TimeUnit.SECONDS
        );
        
        logger.info("Clock synchronizer started for server {} with interval {} seconds", 
            serverId, syncIntervalSeconds);
        fileLogger.info("ClockSynchronizer started with sync interval " + syncIntervalSeconds + " seconds");
    }
    
    /**
     * Stop periodic clock synchronization
     */
    public void stop() {
        fileLogger.info("ClockSynchronizer stopping");
        if (syncTask != null) {
            syncTask.cancel(false);
        }
        syncExecutor.shutdown();
        try {
            if (!syncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                syncExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            syncExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Clock synchronizer stopped for server {}", serverId);
        fileLogger.close();
    }
    
    /**
     * Synchronize clock with NTP servers
     */
    private void syncWithNTP() {
        for (String ntpServer : NTP_SERVERS) {
            try {
                long offset = queryNTPServer(ntpServer);
                this.clockOffset = offset;
                this.lastSyncTime = System.currentTimeMillis();
                this.synchronized_flag = true;
                this.lastSyncError = null;
                
                logger.debug("Clock sync with {} for server {}: offset = {} ms", 
                    ntpServer, serverId, offset);
                fileLogger.info("NTP sync SUCCESS: server=" + ntpServer + ", offset=" + offset + "ms");
                return;
                
            } catch (Exception e) {
                logger.debug("Failed to sync with NTP server {} for server {}: {}", 
                    ntpServer, serverId, e.getMessage());
                fileLogger.debug("NTP sync attempt failed: server=" + ntpServer + ", error=" + e.getMessage());
                lastSyncError = e;
            }
        }
        
        synchronized_flag = false;
        logger.warn("Failed to synchronize clock with any NTP server for server {}", serverId);
        fileLogger.error("NTP sync FAILED: All NTP servers unreachable", null);
    }
    
    /**
     * Query a single NTP server for time offset
     */
    private long queryNTPServer(String hostName) throws Exception {
        DatagramSocket socket = new DatagramSocket();
        try {
            socket.setSoTimeout(TIMEOUT_MS);
            
            InetAddress address = InetAddress.getByName(hostName);
            byte[] buffer = new byte[NTP_PACKET_SIZE];
            
            // Prepare NTP request packet (client mode)
            buffer[0] = (byte)((3 << 6) | NTP_VERSION); // LI (0) + VN (3) + Mode (3)
            
            DatagramPacket request = new DatagramPacket(buffer, buffer.length, address, NTP_PORT);
            socket.send(request);
            
            // Receive NTP response
            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            long clientReceiveTime = System.currentTimeMillis();
            socket.receive(response);
            
            // Extract server timestamp from response
            // Transmit timestamp is at bytes 40-44 (seconds) and 44-48 (fraction)
            long transmitSeconds = readLong(buffer, 40);
            long transmitFraction = readLong(buffer, 44);
            
            // Convert NTP timestamp to milliseconds
            long ntpTimeMs = ((transmitSeconds - NTP_EPOCH_OFFSET) * 1000) + 
                            ((transmitFraction * 1000) >> 32);
            
            // Calculate offset (negative means local clock is ahead)
            long offset = ntpTimeMs - clientReceiveTime;
            
            return offset;
            
        } finally {
            socket.close();
        }
    }
    
    /**
     * Read 32-bit big-endian long from buffer at offset
     */
    private long readLong(byte[] buffer, int offset) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value = (value << 8) | (buffer[offset + i] & 0xFF);
        }
        return value;
    }
    
    /**
     * Get current time synchronized with NTP
     */
    public long getCurrentTimeMillis() {
        long rawTime = System.currentTimeMillis();
        if (synchronized_flag && Math.abs(clockOffset) < 30000) { // Only apply if offset < 30 seconds
            return rawTime + clockOffset;
        }
        return rawTime; // Fall back to system time if sync failed or offset too large
    }
    
    /**
     * Get current time as Instant synchronized with NTP
     */
    public Instant getCurrentTimeInstant() {
        return Instant.ofEpochMilli(getCurrentTimeMillis());
    }
    
    /**
     * Get clock offset from NTP in milliseconds
     */
    public long getClockOffsetMillis() {
        return clockOffset;
    }
    
    /**
     * Check if clock is synchronized
     */
    public boolean isSynchronized() {
        return synchronized_flag;
    }
    
    /**
     * Get time since last successful sync
     */
    public long getTimeSinceLastSyncSeconds() {
        if (lastSyncTime == 0) {
            return -1;
        }
        return (System.currentTimeMillis() - lastSyncTime) / 1000;
    }
    
    /**
     * Force immediate synchronization
     */
    public void forceSync() {
        syncWithNTP();
    }
    
    /**
     * Get synchronization status
     */
    public Map<String, Object> getSyncStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("serverId", serverId);
        status.put("synchronized", synchronized_flag);
        status.put("clockOffsetMs", clockOffset);
        status.put("timeSinceLastSyncSeconds", getTimeSinceLastSyncSeconds());
        status.put("lastError", lastSyncError != null ? lastSyncError.getMessage() : "none");
        return status;
    }
}
