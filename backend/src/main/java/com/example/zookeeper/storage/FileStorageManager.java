package com.example.zookeeper.storage;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.zookeeper.timesync.TimeSyncService;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages file storage operations including local file storage and ZooKeeper metadata.
 * Handles replication, failure detection, and file operations.
 */
public class FileStorageManager {
    private static final Logger logger = LoggerFactory.getLogger(FileStorageManager.class);
    
    // ZooKeeper paths
    private static final String FILES_PATH = "/files";
    private static final String LOCKS_PATH = "/locks";
    private static final String REPLICATION_FACTOR = "3";
    
    private final ZooKeeper zooKeeper;
    private final String serverId;
    private final Path storageRoot;
    private final Path storageDirectory;
    private final Map<String, ReentrantReadWriteLock> fileLocks;
    private final Map<String, FileMetadata> metadataCache;
    private volatile TimeSyncService timeSyncService; // Optional time synchronization service
    
    public FileStorageManager(ZooKeeper zooKeeper, String serverId, String storagePath) throws IOException {
        this(zooKeeper, serverId, storagePath, null);
    }
    
    public FileStorageManager(ZooKeeper zooKeeper, String serverId, String storagePath, 
                             TimeSyncService timeSyncService) throws IOException {
        this.zooKeeper = zooKeeper;
        this.serverId = serverId;
        this.timeSyncService = timeSyncService;
        this.storageRoot = Paths.get(storagePath).toAbsolutePath().normalize();
        this.storageDirectory = storageRoot.resolve(serverId);
        this.fileLocks = new ConcurrentHashMap<>();
        this.metadataCache = new ConcurrentHashMap<>();
        
        // Create storage directory
        Files.createDirectories(storageDirectory);
        
        // Initialize ZooKeeper paths
        initializeZooKeeperPaths();
        
        logger.info("FileStorageManager initialized for server {} at {} (timeSync: {})", 
                   serverId, storageDirectory, timeSyncService != null ? "enabled" : "disabled");
    }
    
    /**
     * Initialize required ZooKeeper paths
     */
    private void initializeZooKeeperPaths() {
        try {
            createPathIfNotExists(FILES_PATH);
            createPathIfNotExists(LOCKS_PATH);
        } catch (Exception e) {
            logger.error("Failed to initialize ZooKeeper paths", e);
        }
    }
    
    private void createPathIfNotExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, new byte[0], 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT);
            logger.info("Created ZooKeeper path: {}", path);
        }
    }
    
    /**
     * Upload a file to the distributed storage system
     */
    public boolean uploadFile(String filename, byte[] content, boolean isLeader) throws Exception {
        // Only leader can coordinate uploads
        if (!isLeader) {
            logger.warn("Non-leader server cannot coordinate uploads");
            return false;
        }
        
        // Check if clock skew allows write operations
        if (timeSyncService != null && !timeSyncService.canPerformWriteOperation()) {
            logger.error("Upload rejected due to clock skew: {}", filename);
            return false;
        }

        // Each upload is a new file identity, even if filename is duplicated.
        FileMetadata metadata = new FileMetadata(filename, content.length, serverId);
        
        // Use synchronized time if available
        if (timeSyncService != null) {
            metadata.setCreatedAt(timeSyncService.getCurrentTimeInstant());
        }
        
        String fileId = metadata.getFileId();
        String lockPath = LOCKS_PATH + "/" + fileId;
        
        // Acquire distributed lock
        if (!acquireLock(lockPath)) {
            logger.warn("Could not acquire lock for fileId: {}", fileId);
            return false;
        }
        
        try {
            String metadataPath = FILES_PATH + "/" + fileId;
            
            // Store file locally on this server
            Path localPath = storageDirectory.resolve(metadata.getFileId());
            Files.write(localPath, content);
            metadata.addLocation(serverId);
            
            // Replicate to other servers
            List<String> aliveServers = getAliveServers();
            List<String> successfulReplicas = replicateFile(metadata.getFileId(), content, aliveServers);
            
            // Add locations from successful replication
            for (String server : successfulReplicas) {
                metadata.addLocation(server);
            }
            
            // Update metadata in ZooKeeper
            byte[] metadataBytes = metadata.toJson().getBytes();
            zooKeeper.create(metadataPath, metadataBytes,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            
            // Update cache
            metadataCache.put(filename, metadata);
            
            logger.info("Uploaded file: {} (ID: {}, size: {} bytes, replicas: {})", 
                    filename, metadata.getFileId(), content.length, metadata.getLocations().size());
            return true;
            
        } finally {
            releaseLock(lockPath);
        }
    }
    
    /**
     * Download a file from the distributed storage system
     */
    public byte[] downloadFile(String filename) throws Exception {
        MetadataNode metadataNode = findLatestMetadataNodeByFilename(filename);
        if (metadataNode == null) {
            logger.warn("File not found: {}", filename);
            return null;
        }

        FileMetadata metadata = metadataNode.metadata;
        
        if (metadata.isDeleted()) {
            logger.warn("File has been deleted: {}", filename);
            return null;
        }
        
        // Try to read from any location that has the file
        for (String location : metadata.getLocations()) {
            if (location.equals(serverId)) {
                // File is local
                Path localPath = storageDirectory.resolve(metadata.getFileId());
                if (Files.exists(localPath)) {
                    logger.info("Downloaded file {} from local storage", filename);
                    return Files.readAllBytes(localPath);
                }
            } else {
                // File is on another server - request it
                byte[] content = requestFileFromServer(location, metadata.getFileId());
                if (content != null) {
                    logger.info("Downloaded file {} from server {}", filename, location);
                    return content;
                }
            }
        }
        
        logger.error("Could not download file {} from any replica", filename);
        return null;
    }
    
    /**
     * Delete a file from the distributed storage system
     */
    public boolean deleteFile(String filename, boolean isLeader) throws Exception {
        if (!isLeader) {
            logger.warn("Only leader can coordinate deletes");
            return false;
        }
        
        // Check if clock skew allows write operations
        if (timeSyncService != null && !timeSyncService.canPerformWriteOperation()) {
            logger.error("Delete rejected due to clock skew: {}", filename);
            return false;
        }

        MetadataNode metadataNode = findLatestMetadataNodeByFilename(filename);
        if (metadataNode == null) {
            logger.warn("File not found for delete: {}", filename);
            return false;
        }

        FileMetadata metadata = metadataNode.metadata;
        String lockPath = LOCKS_PATH + "/" + metadata.getFileId();

        if (!acquireLock(lockPath)) {
            logger.warn("Could not acquire lock for delete: {}", filename);
            return false;
        }

        try {
            String metadataPath = FILES_PATH + "/" + metadataNode.nodeId;

            // Delete all known replicas from storage.
            for (String location : metadata.getLocations()) {
                deleteReplicaFile(location, metadata.getFileId());
            }

            // Delete metadata node so filename is fully removed from namespace.
            zooKeeper.delete(metadataPath, metadataNode.stat.getVersion());
            
            // Remove from cache
            metadataCache.remove(filename);

            logger.info("Deleted file: {}", filename);
            return true;
            
        } finally {
            releaseLock(lockPath);
        }
    }
    
    /**
     * List all files in the storage system
     */
    public List<FileMetadata> listFiles() throws Exception {
        List<FileMetadata> files = new ArrayList<>();
        
        List<String> children = zooKeeper.getChildren(FILES_PATH, false);
        
        for (String filename : children) {
            String metadataPath = FILES_PATH + "/" + filename;
            Stat stat = zooKeeper.exists(metadataPath, false);
            
            if (stat != null) {
                byte[] data = zooKeeper.getData(metadataPath, false, stat);
                FileMetadata metadata = FileMetadata.fromJson(new String(data));
                
                if (!metadata.isDeleted()) {
                    files.add(metadata);
                }
            }
        }
        
        return files;
    }
}