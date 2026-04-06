package com.example.zookeeper.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents metadata for a file stored in the distributed storage system.
 * This is stored in ZooKeeper to track file locations and versions.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadata {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    
    private String fileId;           // Unique identifier for the file
    private String filename;         // Original filename
    private long size;               // File size in bytes
    private Instant createdAt;       // Creation timestamp
    private Instant modifiedAt;      // Last modification timestamp
    private String owner;            // Server ID that owns the file
    private int version;             // Version number for conflict detection
    private List<String> locations;  // Server IDs where file is stored
    private boolean deleted;          // Soft delete flag
    
    public FileMetadata(String filename, long size, String owner) {
        this.fileId = UUID.randomUUID().toString();
        this.filename = filename;
        this.size = size;
        this.createdAt = Instant.now();
        this.modifiedAt = Instant.now();
        this.owner = owner;
        this.locations = new ArrayList<>();
        this.version = 1;
        this.deleted = false;
    }
    
    public void addLocation(String serverId) {
        if (!locations.contains(serverId)) {
            locations.add(serverId);
        }
    }
    
    public void removeLocation(String serverId) {
        locations.remove(serverId);
    }
    
    public String toJson() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize metadata", e);
        }
    }
    
    public static FileMetadata fromJson(String json) {
        try {
            return objectMapper.readValue(json, FileMetadata.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize metadata", e);
        }
    }
    
    @Override
    public String toString() {
        return String.format("FileMetadata{id='%s', name='%s', size=%d, version=%d, locations=%s}",
                fileId, filename, size, version, locations);
    }
}
