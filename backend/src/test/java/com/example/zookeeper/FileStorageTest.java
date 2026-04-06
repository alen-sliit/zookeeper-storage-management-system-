package com.example.zookeeper.test;

import com.example.zookeeper.storage.FileMetadata;
import com.example.zookeeper.storage.FileStorageManager;
import org.apache.zookeeper.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileStorageTest {

    @TempDir
    Path tempDir;

    private ZooKeeper zooKeeper;
    private FileStorageManager storageManager;
    private String serverId = "test-server-1";

    @BeforeAll
    void setup() throws Exception {
        String connectString = "localhost:2181";
        zooKeeper = new ZooKeeper(connectString, 3000, watchedEvent -> {
        });

        Thread.sleep(1000);

        storageManager = new FileStorageManager(zooKeeper, serverId, tempDir.toString());
    }

    @AfterAll
    void cleanup() throws Exception {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    @Test
    void testUploadAndDownload() throws Exception {
        String filename = "test.txt";
        String content = "Hello, Distributed Storage!";

        // Upload file
        boolean uploadSuccess = storageManager.uploadFile(filename, content.getBytes(), true);
        assertTrue(uploadSuccess, "Upload should succeed");

        // Download file
        byte[] downloaded = storageManager.downloadFile(filename);
        assertNotNull(downloaded, "Downloaded content should not be null");
        assertEquals(content, new String(downloaded), "Downloaded content should match uploaded content");
    }

    @Test
    void testListFiles() throws Exception {
        storageManager.uploadFile("file1.txt", "Content 1".getBytes(), true);
        storageManager.uploadFile("file2.txt", "Content 2".getBytes(), true);

        List<FileMetadata> files = storageManager.listFiles();
        assertTrue(files.size() >= 2, "Should list at least 2 files");

        boolean found1 = files.stream().anyMatch(f -> f.getFilename().equals("file1.txt"));
        boolean found2 = files.stream().anyMatch(f -> f.getFilename().equals("file2.txt"));

        assertTrue(found1 && found2, "Both uploaded files should be listed");
    }

    @Test
    void testDeleteFile() throws Exception {
        String filename = "to_delete.txt";
        storageManager.uploadFile(filename, "Delete me".getBytes(), true);
        
        assertNotNull(storageManager.downloadFile(filename));
        
        boolean deleteSuccess = storageManager.deleteFile(filename, true);
        assertTrue(deleteSuccess, "Delete should succeed");
        
        assertNull(storageManager.downloadFile(filename), "File should be deleted");
    }
      @Test
    void testMetadataStructure() throws Exception {
        String filename = "metadata_test.txt";
        String content = "Test metadata";
        
        storageManager.uploadFile(filename, content.getBytes(), true);
        
        List<FileMetadata> files = storageManager.listFiles();
        FileMetadata metadata = files.stream()
                .filter(f -> f.getFilename().equals(filename))
                .findFirst()
                .orElse(null);
        
        assertNotNull(metadata, "Metadata should exist");
        assertNotNull(metadata.getFileId(), "File ID should be set");
        assertNotNull(metadata.getCreatedAt(), "Created timestamp should be set");
        assertNotNull(metadata.getModifiedAt(), "Modified timestamp should be set");
        assertEquals(content.length(), metadata.getSize(), "Size should match");
        assertTrue(metadata.getLocations().size() > 0, "Should have at least one location");
    }
    
    @Test
    void testReplication() throws Exception {
        String filename = "replicated.txt";
        String content = "This file should be replicated";
        
        storageManager.uploadFile(filename, content.getBytes(), true);
        List<FileMetadata> files = storageManager.listFiles();
        FileMetadata metadata = files.stream()
                .filter(f -> f.getFilename().equals(filename))
                .findFirst()
                .orElse(null);
        
        assertNotNull(metadata, "Metadata should exist");
        assertTrue(metadata.getLocations().size() >= 1, "File should be replicated to at least one server");
    }
}

