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

    
    /**
     * Synchronize file replicas when this server joins the cluster.
     */
    public void synchronizeOnJoin() {
        try {
            List<String> children = zooKeeper.getChildren(FILES_PATH, false);
            int restored = 0;
            int addedReplica = 0;
            int targetReplication = Integer.parseInt(REPLICATION_FACTOR);

            for (String nodeId : children) {
                String metadataPath = FILES_PATH + "/" + nodeId;
                Stat stat = zooKeeper.exists(metadataPath, false);
                if (stat == null) {
                    continue;
                }

                byte[] data = zooKeeper.getData(metadataPath, false, stat);
                FileMetadata metadata = FileMetadata.fromJson(new String(data));
                if (metadata.isDeleted()) {
                    continue;
                }

                Path localPath = storageDirectory.resolve(metadata.getFileId());
                boolean localExists = Files.exists(localPath);
                boolean listedOnThisServer = metadata.getLocations().contains(serverId);

                if (listedOnThisServer && !localExists) {
                    byte[] content = readFromAnyReplica(metadata);
                    if (content != null) {
                        Files.write(localPath, content);
                        restored++;
                    }
                    continue;
                }

                if (!listedOnThisServer && metadata.getLocations().size() < targetReplication) {
                    byte[] content = readFromAnyReplica(metadata);
                    if (content != null) {
                        Files.write(localPath, content);
                        metadata.addLocation(serverId);
                        updateMetadata(nodeId, metadata);
                        addedReplica++;
                    }
                }
            }

            logger.info("Join sync complete for server {}: restored={}, addedReplicas={}",
                    serverId, restored, addedReplica);
        } catch (Exception e) {
            logger.error("Failed to synchronize files on join for server {}", serverId, e);
        }
    }
    
    /**
     * Replicate a file to multiple servers for fault tolerance
     */
    private List<String> replicateFile(String fileId, byte[] content, List<String> targetServers) {
        int replicationCount = Math.min(Integer.parseInt(REPLICATION_FACTOR), targetServers.size());
        List<String> successfulReplicas = new ArrayList<>();
        
        for (int i = 0; i < replicationCount; i++) {
            String targetServer = targetServers.get(i);
            if (!targetServer.equals(serverId)) {
                try {
                    sendFileToServer(targetServer, fileId, content);
                    successfulReplicas.add(targetServer);
                    logger.info("Replicated file {} to server {}", fileId, targetServer);
                } catch (Exception e) {
                    logger.error("Failed to replicate file {} to server {}", fileId, targetServer, e);
                }
            }
        }

        return successfulReplicas;
    }
    
    /**
     * Get list of alive servers (excluding self)
     */
    private List<String> getAliveServers() {
        List<String> aliveServers = new ArrayList<>();
        // This should integrate with your existing membership system
        // For now, return a list of known servers
        try {
            List<String> servers = zooKeeper.getChildren("/storage-servers", false);
            for (String server : servers) {
                if (!server.equals(serverId)) {
                    aliveServers.add(server);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get alive servers", e);
        }
        return aliveServers;
    }

    private List<String> getAliveServersIncludingSelf() {
        List<String> aliveServers = new ArrayList<>();
        try {
            aliveServers.addAll(zooKeeper.getChildren("/storage-servers", false));
        } catch (Exception e) {
            logger.error("Failed to get alive servers", e);
        }
        return aliveServers;
    }

    private byte[] readFromAnyReplica(FileMetadata metadata) {
        for (String location : metadata.getLocations()) {
            try {
                if (location.equals(serverId)) {
                    Path localPath = storageDirectory.resolve(metadata.getFileId());
                    if (Files.exists(localPath)) {
                        return Files.readAllBytes(localPath);
                    }
                } else {
                    byte[] content = requestFileFromServer(location, metadata.getFileId());
                    if (content != null) {
                        return content;
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed reading replica {} for file {}", location, metadata.getFilename(), e);
            }
        }
        return null;
    }
    
    /**
     * Request a file from another server (simplified - would use HTTP/gRPC in production)
     */
    private byte[] requestFileFromServer(String serverId, String fileId) {
        // In a real implementation, this would make an HTTP/gRPC call
        // For now, simulate by reading from local storage if available
        Path otherServerPath = getServerStoragePath(serverId).resolve(fileId);
        try {
            if (Files.exists(otherServerPath)) {
                return Files.readAllBytes(otherServerPath);
            }
        } catch (IOException e) {
            logger.error("Failed to read file from server {}: {}", serverId, fileId, e);
        }
        return null;
    }
    
    /**
     * Send file to another server (simplified)
     */
    private void sendFileToServer(String targetServer, String fileId, byte[] content) {
        // In a real implementation, this would make an HTTP/gRPC call
        // For simulation, write to a shared storage location
        Path targetPath = getServerStoragePath(targetServer).resolve(fileId);
        try {
            Files.createDirectories(targetPath.getParent());
            Files.write(targetPath, content);
        } catch (IOException e) {
            throw new RuntimeException("Failed to send file to server", e);
        }
    }

    /**
     * Delete file replica from the target server's simulated storage directory.
     */
    private void deleteReplicaFile(String targetServer, String fileId) {
        Path replicaPath = targetServer.equals(serverId)
                ? storageDirectory.resolve(fileId)
                : getServerStoragePath(targetServer).resolve(fileId);

        try {
            boolean deleted = Files.deleteIfExists(replicaPath);
            if (deleted) {
                logger.info("Deleted replica {} on server {}", fileId, targetServer);
            } else {
                logger.debug("Replica {} not found on server {}", fileId, targetServer);
            }
        } catch (IOException e) {
            logger.error("Failed to delete replica {} on server {}", fileId, targetServer, e);
        }
    }

    private Path getServerStoragePath(String serverName) {
        return storageRoot.resolve(serverName);
    }
    
    /**
     * Acquire distributed lock using ZooKeeper
     */
    private boolean acquireLock(String lockPath) {
        try {
            String createdPath = zooKeeper.create(lockPath, serverId.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            
            // Also track lock locally for cleanup
            fileLocks.putIfAbsent(lockPath, new ReentrantReadWriteLock());
            
            logger.debug("Acquired lock: {}", lockPath);
            return true;
            
        } catch (KeeperException.NodeExistsException e) {
            // Lock is held by someone else
            logger.debug("Lock already held: {}", lockPath);
            return false;
        } catch (Exception e) {
            logger.error("Failed to acquire lock: {}", lockPath, e);
            return false;
        }
    }
    
    /**
     * Release distributed lock
     */
    private void releaseLock(String lockPath) {
        try {
            zooKeeper.delete(lockPath, -1);
            fileLocks.remove(lockPath);
            logger.debug("Released lock: {}", lockPath);
        } catch (Exception e) {
            logger.error("Failed to release lock: {}", lockPath, e);
        }
    }
    
    /**
     * Handle server failure - reassign files from dead server
     */
    public void handleServerFailure(String deadServerId) throws Exception {
        logger.warn("Handling failure of server: {}", deadServerId);
        
        List<FileMetadata> affectedFiles = new ArrayList<>();
        
        // Find all files that were on the dead server
        List<String> files = zooKeeper.getChildren(FILES_PATH, false);
        for (String nodeId : files) {
            String metadataPath = FILES_PATH + "/" + nodeId;
            Stat stat = zooKeeper.exists(metadataPath, false);
            
            if (stat != null) {
                byte[] data = zooKeeper.getData(metadataPath, false, stat);
                FileMetadata metadata = FileMetadata.fromJson(new String(data));
                
                if (metadata.getLocations().contains(deadServerId)) {
                    affectedFiles.add(metadata);
                }
            }
        }
        
        // For each affected file, re-replicate to available servers
        List<String> aliveServers = getAliveServersIncludingSelf();
        for (FileMetadata metadata : affectedFiles) {
            metadata.removeLocation(deadServerId);

            int targetReplication = Integer.parseInt(REPLICATION_FACTOR);
            while (metadata.getLocations().size() < targetReplication) {
                String newServer = null;
                for (String candidate : aliveServers) {
                    if (!metadata.getLocations().contains(candidate)) {
                        newServer = candidate;
                        break;
                    }
                }

                if (newServer == null) {
                    break;
                }

                byte[] content = readFromAnyReplica(metadata);
                if (content == null) {
                    logger.error("Cannot re-replicate file {}: no readable source replica", metadata.getFilename());
                    break;
                }

                if (newServer.equals(serverId)) {
                    Path localPath = storageDirectory.resolve(metadata.getFileId());
                    Files.write(localPath, content);
                } else {
                    sendFileToServer(newServer, metadata.getFileId(), content);
                }
                metadata.addLocation(newServer);
            }

            updateMetadata(metadata.getFileId(), metadata);
        }
        
        logger.info("Handled failure of server {}: {} files affected", 
                deadServerId, affectedFiles.size());
    }

     
    /**
     * Update file metadata in ZooKeeper
     */
    private void updateMetadata(String nodeId, FileMetadata metadata) throws Exception {
        String metadataPath = FILES_PATH + "/" + nodeId;
        Stat stat = zooKeeper.exists(metadataPath, false);
        
        if (stat != null) {
            zooKeeper.setData(metadataPath, metadata.toJson().getBytes(), stat.getVersion());
            metadataCache.put(metadata.getFilename(), metadata);
        }
    }

    private MetadataNode findLatestMetadataNodeByFilename(String filename) throws Exception {
        List<String> children = zooKeeper.getChildren(FILES_PATH, false);
        MetadataNode latest = null;

        for (String nodeId : children) {
            String metadataPath = FILES_PATH + "/" + nodeId;
            Stat stat = zooKeeper.exists(metadataPath, false);
            if (stat == null) {
                continue;
            }

            byte[] data = zooKeeper.getData(metadataPath, false, stat);
            FileMetadata metadata = FileMetadata.fromJson(new String(data));

            if (metadata.isDeleted() || !filename.equals(metadata.getFilename())) {
                continue;
            }

            if (latest == null || isNewer(metadata, latest.metadata)) {
                latest = new MetadataNode(nodeId, stat, metadata);
            }
        }

        return latest;
    }

    private boolean isNewer(FileMetadata candidate, FileMetadata current) {
        Instant candidateTime = candidate.getModifiedAt() != null ? candidate.getModifiedAt() : candidate.getCreatedAt();
        Instant currentTime = current.getModifiedAt() != null ? current.getModifiedAt() : current.getCreatedAt();
        if (candidateTime == null) {
            return false;
        }
        if (currentTime == null) {
            return true;
        }
        return candidateTime.isAfter(currentTime);
    }

    private static class MetadataNode {
        private final String nodeId;
        private final Stat stat;
        private final FileMetadata metadata;

        private MetadataNode(String nodeId, Stat stat, FileMetadata metadata) {
            this.nodeId = nodeId;
            this.stat = stat;
            this.metadata = metadata;
        }
    }
    
    /**
     * Get storage statistics
     */
    public Map<String, Object> getStatistics() throws Exception {
        Map<String, Object> stats = new HashMap<>();
        
        long totalSize = 0;
        int fileCount = 0;
        
        List<FileMetadata> files = listFiles();
        for (FileMetadata file : files) {
            totalSize += file.getSize();
            fileCount++;
        }
        
        stats.put("fileCount", fileCount);
        stats.put("totalSizeBytes", totalSize);
        stats.put("serverId", serverId);
        stats.put("storageDirectory", storageDirectory.toString());
        
        // Include time sync status if available
        if (timeSyncService != null) {
            stats.put("timeSyncHealthy", timeSyncService.isTimeSyncHealthy());
            stats.put("clockSkewMs", timeSyncService.getMaxClockSkewMs());
        }
        
        return stats;
    }
    
    /**
     * Set TimeSyncService for synchronized timestamps
     */
    public void setTimeSyncService(TimeSyncService timeSyncService) {
        this.timeSyncService = timeSyncService;
        logger.info("TimeSyncService set for FileStorageManager on server {}", serverId);
    }
    
    /**
     * Get TimeSyncService
     */
    public TimeSyncService getTimeSyncService() {
        return timeSyncService;
    }
    
    /**
     * Get current synchronized time
     */
    public Instant getCurrentTime() {
        if (timeSyncService != null) {
            return timeSyncService.getCurrentTimeInstant();
        }
        return Instant.now();
    }
    
    /**
     * Log time synchronization status
     */
    public void logTimeSyncStatus() {
        if (timeSyncService != null) {
            logger.info(timeSyncService.getHealthReport());
        } else {
            logger.info("TimeSyncService not configured for server {}", serverId);
        }
    }
}