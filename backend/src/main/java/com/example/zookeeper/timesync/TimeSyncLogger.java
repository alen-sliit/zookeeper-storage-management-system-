package com.example.zookeeper.timesync;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Simple file logger for time synchronization events.
 * Writes to backend/logs/timesync.log with timestamps and rotation support.
 */
public class TimeSyncLogger {
    
    private static final String LOG_DIR = "logs";
    private static final String LOG_FILE = "timesync.log";
    private static final long MAX_LOG_SIZE = 10 * 1024 * 1024; // 10 MB
    private static final DateTimeFormatter TIMESTAMP_FORMAT = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
    
    private final Path logPath;
    private final String serverId;
    private volatile PrintWriter writer;
    private volatile long currentLogSize = 0;
    private final Object lock = new Object();
    
    public TimeSyncLogger(String serverId) {
        this.serverId = serverId;
        this.logPath = Paths.get(LOG_DIR, LOG_FILE);
        
        try {
            // Create logs directory if it doesn't exist
            Files.createDirectories(Paths.get(LOG_DIR));
            
            // Check if we need to rotate
            if (Files.exists(logPath)) {
                this.currentLogSize = Files.size(logPath);
                if (currentLogSize >= MAX_LOG_SIZE) {
                    rotateLog();
                }
            }
            
            // Open file in append mode
            this.writer = new PrintWriter(
                new FileWriter(logPath.toFile(), true), 
                true
            );
            
        } catch (IOException e) {
            System.err.println("Failed to initialize TimeSyncLogger: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Log an info message with timestamp
     */
    public void info(String message) {
        logMessage("INFO", message);
    }
    
    /**
     * Log a debug message with timestamp
     */
    public void debug(String message) {
        logMessage("DEBUG", message);
    }
    
    /**
     * Log a warning message with timestamp
     */
    public void warn(String message) {
        logMessage("WARN", message);
    }
    
    /**
     * Log an error message with timestamp
     */
    public void error(String message, Exception e) {
        String msg = message;
        if (e != null) {
            msg += " - " + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        logMessage("ERROR", msg);
    }
    
    /**
     * Internal method to write log entry
     */
    private void logMessage(String level, String message) {
        synchronized (lock) {
            if (writer == null) {
                return;
            }
            
            try {
                String timestamp = TIMESTAMP_FORMAT.format(Instant.now());
                String logEntry = String.format("[%s] [%s] [%s] %s", 
                    timestamp, level, serverId, message);
                
                writer.println(logEntry);
                writer.flush();
                
                currentLogSize += logEntry.length() + System.lineSeparator().length();
                
                // Rotate if size exceeded
                if (currentLogSize >= MAX_LOG_SIZE) {
                    rotateLog();
                }
                
            } catch (Exception e) {
                System.err.println("Error writing to timesync log: " + e.getMessage());
            }
        }
    }
    
    /**
     * Rotate log file (rename current to .1, .2, etc.)
     */
    private void rotateLog() {
        try {
            if (writer != null) {
                writer.close();
            }
            
            // Find next available backup number
            int backupNum = 1;
            Path backupPath;
            while ((backupPath = Paths.get(LOG_DIR, LOG_FILE + "." + backupNum)).toFile().exists()) {
                backupNum++;
            }
            
            // Rename current log to backup
            Files.move(logPath, backupPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            // Reopen for new log
            this.writer = new PrintWriter(
                new FileWriter(logPath.toFile(), true),
                true
            );
            this.currentLogSize = 0;
            
            writer.println("=== Log rotated ===");
            writer.flush();
            
        } catch (IOException e) {
            System.err.println("Failed to rotate log: " + e.getMessage());
        }
    }
    
    /**
     * Close the logger
     */
    public void close() {
        synchronized (lock) {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }
    }
    
    /**
     * Get the log file path
     */
    public String getLogFilePath() {
        return logPath.toAbsolutePath().toString();
    }
}
