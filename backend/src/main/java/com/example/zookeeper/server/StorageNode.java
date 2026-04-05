package com.example.zookeeper.server;


import com.example.zookeeper.config.ServerConfig;
import com.example.zookeeper.election.ConsensusMonitor;
import com.example.zookeeper.election.LeaderElection;
import com.example.zookeeper.storage.FileMetadata;
import com.example.zookeeper.storage.FileStorageManager;
import com.example.zookeeper.timesync.TimeSyncService;
import com.example.zookeeper.zookeeper.ZookeeperConnection;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * Main class for each storage node in the distributed file system.
 * Each node runs as a separate process and participates in leader election.
 */
public class StorageNode {

    private static final Logger logger = LoggerFactory.getLogger(StorageNode.class);
    
    private final ServerConfig config;
    private ZookeeperConnection zkConnection;
    private ZooKeeper zooKeeper;
    private LeaderElection leaderElection;
    private ConsensusMonitor monitor;
    private FileStorageManager storageManager;
    private TimeSyncService timeSyncService;
    private boolean isLeader = false;
    private final String storagePath;
    private final Set<String> knownServers = new HashSet<>();
    private boolean running = true;
    
    public StorageNode(ServerConfig config) {
        this.config = config;
        this.storagePath = config.getStoragePath() != null ? config.getStoragePath() : "storage";
    }
    
    /**
     * Start the storage node.
     */
    public void start() throws Exception {
        System.out.println("\n========================================");
        System.out.println("Starting Storage Node: " + config.getServerId());
        System.out.println("ZooKeeper: " + config.getZookeeperAddress());
        System.out.println("Storage Path: " + config.getStoragePath());
        System.out.println("========================================\n");
        
        // Create storage directory if it doesn't exist
        createStorageDirectory();
        
        // Connect to ZooKeeper
        zkConnection = new ZookeeperConnection();
        zooKeeper = zkConnection.connect(config.getZookeeperAddress());
        
        // Initialize leader election
        leaderElection = new LeaderElection(zooKeeper, config.getServerId());
        leaderElection.initialize();

        // Initialize consensus monitor
        monitor = new ConsensusMonitor(zooKeeper, config.getServerId());

        // Initialize time synchronization service (Member 3 implementation)
        try {
            timeSyncService = new TimeSyncService(config.getServerId(), zooKeeper, 300, 30);
            timeSyncService.start();
            logger.info("TimeSyncService started for server {}", config.getServerId());
        } catch (Exception e) {
            logger.warn("Failed to start TimeSyncService for server {}, continuing without time sync", 
                       config.getServerId(), e);
            timeSyncService = null;
        }

        // Initialize storage manager with time sync service
        storageManager = new FileStorageManager(zooKeeper, config.getServerId(), storagePath, timeSyncService);

        // Recover/align local replicas for existing files when this server joins.
        storageManager.synchronizeOnJoin();

        // Initialize and watch leadership/server membership
        updateLeaderStatus();
        watchLeadership();
        watchServerFailures();
        
        // Print initial status
        printStatus();
        
        // Start interactive console in background thread
        Thread interactiveThread = new Thread(() -> {
            try {
                startInteractiveMode();
            } catch (Exception e) {
                logger.warn("Interactive mode error: {}", e.getMessage());
            }
        });
        interactiveThread.setName("InteractiveConsole-" + config.getServerId());
        interactiveThread.setDaemon(false);
        interactiveThread.start();
        
        // Keep the node running - don't exit
        while (running) {
            Thread.sleep(10000);  // Check every 10 seconds if still running
        }
    }
    
    /**
     * Create local storage directory.
     */
    private void createStorageDirectory() {
        Path dir = Paths.get(storagePath);
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
                System.out.println("Created storage directory: " + storagePath);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create storage directory", e);
            }
        }
    }
    
    /**
     * Print current server status.
     */
    private void printStatus() {
        try {
            System.out.println("\n--- Current Status ---");
            System.out.println("Server ID: " + config.getServerId());
            updateLeaderStatus();
            System.out.println("Role: " + (isLeader ? "LEADER" : "FOLLOWER"));
            System.out.println("Active Servers: " + leaderElection.getActiveServers());
            System.out.println("Current Leader: " + leaderElection.getCurrentLeader());
            System.out.println("----------------------\n");
        } catch (Exception e) {
            System.err.println("Error getting status: " + e.getMessage());
        }
    }
    
    /**
     * Print time synchronization status
     */
    private void printTimeSyncStatus() {
        if (timeSyncService != null) {
            System.out.println(timeSyncService.getHealthReport());
        } else {
            System.out.println("TimeSyncService not initialized for server " + config.getServerId());
        }
    }

    private void watchLeadership() {
        try {
            String leaderCurrentPath = "/leader/current";
            zooKeeper.exists(leaderCurrentPath, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted
                        || event.getType() == Watcher.Event.EventType.NodeCreated
                        || event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    logger.info("Leader node changed, refreshing leader status");
                    try {
                        leaderElection.electLeader();
                        updateLeaderStatus();
                        watchLeadership();
                    } catch (Exception e) {
                        logger.error("Failed to refresh leader status", e);
                    }
                }
            });

            Stat stat = zooKeeper.exists(leaderCurrentPath, false);
            if (stat != null) {
                byte[] data = zooKeeper.getData(leaderCurrentPath, false, stat);
                isLeader = new String(data).equals(config.getServerId());
            } else {
                updateLeaderStatus();
            }
        } catch (Exception e) {
            logger.error("Failed to watch leadership", e);
        }
    }

    private void updateLeaderStatus() {
        isLeader = leaderElection != null && leaderElection.isLeader();
        logger.info("Server {} is now {}leader", config.getServerId(), isLeader ? "" : "not ");
    }

    private void watchServerFailures() {
        try {
            String serversPath = "/storage-servers";
            knownServers.clear();
            knownServers.addAll(zooKeeper.getChildren(serversPath, false));

            zooKeeper.getChildren(serversPath, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    logger.info("Server list changed, checking for failures");
                    checkForServerFailures();
                    watchServerFailures();
                }
            });
        } catch (Exception e) {
            logger.error("Failed to watch server failures", e);
        }
    }

    private void checkForServerFailures() {
        try {
            List<String> currentServers = zooKeeper.getChildren("/storage-servers", false);
            logger.info("Current servers: {}", currentServers);

            if (isLeader && storageManager != null) {
                Set<String> currentSet = new HashSet<>(currentServers);
                for (String previous : knownServers) {
                    if (!currentSet.contains(previous)) {
                        logger.warn("Detected failed server: {}", previous);
                        storageManager.handleServerFailure(previous);
                    }
                }
            }

            knownServers.clear();
            knownServers.addAll(currentServers);
        } catch (Exception e) {
            logger.error("Failed to check server failures", e);
        }
    }

    public void printHealthReport() {
        if (monitor != null) {
            System.out.println(monitor.getHealthReport());
        }
    }
    
    /**
     * Run interactive console for testing.
     */
    public void startInteractiveMode() {
        logger.info("Starting interactive mode for StorageNode {}", config.getServerId());
        System.out.println("\n=== Distributed Storage Node " + config.getServerId() + " ===");
        System.out.println("Commands: put <filename> [content] | putfile <source-path> [filename] | get <filename> | getfile <filename> <destination-path> | delete <filename> | list | status | stats | leader | servers | health | timesync | quit");
        System.out.println("Current role: " + (isLeader ? "LEADER" : "FOLLOWER"));

        try (Scanner scanner = new Scanner(System.in)) {
            while (running) {
                System.out.print("> ");
                // Check if there's input available, gracefully exit if stdin is EOF (e.g., when running in background)
                if (!scanner.hasNextLine()) {
                    System.out.println("\nStdin closed. Node will continue running in background...");
                    break;
                }
                String input = scanner.nextLine().trim();
                if (input.isEmpty()) {
                    continue;
                }

                String[] parts = input.split(" ", 2);
                String command = parts[0].toLowerCase();
                
                try {
                    switch (command) {
                        case "put":
                            if (parts.length < 2) {
                                System.out.println("Usage: put <filename> [content]");
                                break;
                            }
                            List<String> putArgs = tokenizeArguments(parts[1]);
                            if (putArgs.isEmpty()) {
                                System.out.println("Usage: put <filename> [content]");
                                break;
                            }
                            String filename = normalizeDistributedFilename(putArgs.get(0));
                            String content = putArgs.size() > 1 ? putArgs.get(1) : "hello world";

                            System.out.println("Uploading file: " + filename);
                            boolean uploadSuccess = storageManager.uploadFile(filename, content.getBytes(), isLeader);
                            System.out.println(uploadSuccess ? "File uploaded successfully" : "Failed to upload file");
                            break;

                        case "putfile":
                            if (parts.length < 2) {
                                System.out.println("Usage: putfile <source-path> [filename]");
                                break;
                            }
                            List<String> putFileArgs = tokenizeArguments(parts[1]);
                            if (putFileArgs.isEmpty()) {
                                System.out.println("Usage: putfile <source-path> [filename]");
                                break;
                            }
                            Path sourcePath = Paths.get(stripWrappingQuotes(putFileArgs.get(0)));

                            if (!Files.exists(sourcePath) || !Files.isRegularFile(sourcePath)) {
                                System.out.println("Source file not found: " + sourcePath);
                                break;
                            }

                            String targetFilename = putFileArgs.size() > 1
                                    ? normalizeDistributedFilename(putFileArgs.get(1))
                                    : sourcePath.getFileName().toString();
                            byte[] fileBytes = Files.readAllBytes(sourcePath);

                            System.out.println("Uploading local file: " + sourcePath + " as " + targetFilename);
                            boolean putFileSuccess = storageManager.uploadFile(targetFilename, fileBytes, isLeader);
                            System.out.println(putFileSuccess
                                    ? "File uploaded successfully (" + fileBytes.length + " bytes)"
                                    : "Failed to upload file");
                            break;

                        case "get":
                            if (parts.length < 2) {
                                System.out.println("Usage: get <filename>");
                                break;
                            }
                            String getFilename = normalizeDistributedFilename(parts[1]);
                            System.out.println("Downloading file: " + getFilename);
                            byte[] fileContent = storageManager.downloadFile(getFilename);

                            if (fileContent != null) {
                                System.out.println("File content: " + new String(fileContent));
                                System.out.println("Size: " + fileContent.length + " bytes");
                            } else {
                                System.out.println("File not found");
                            }
                            break;

                        case "getfile":
                            if (parts.length < 2) {
                                System.out.println("Usage: getfile <filename> <destination-path>");
                                break;
                            }
                            List<String> getFileArgs = tokenizeArguments(parts[1]);
                            if (getFileArgs.size() < 2) {
                                System.out.println("Usage: getfile <filename> <destination-path>");
                                break;
                            }

                            String remoteFilename = normalizeDistributedFilename(getFileArgs.get(0));
                            Path destinationPath = Paths.get(stripWrappingQuotes(getFileArgs.get(1)));
                            byte[] downloadedBytes = storageManager.downloadFile(remoteFilename);

                            if (downloadedBytes == null) {
                                System.out.println("File not found");
                                break;
                            }

                            if (Files.isDirectory(destinationPath)) {
                                destinationPath = destinationPath.resolve(remoteFilename);
                            }

                            Path parent = destinationPath.getParent();
                            if (parent != null) {
                                Files.createDirectories(parent);
                            }

                            Files.write(destinationPath, downloadedBytes);
                            System.out.println("Saved file to: " + destinationPath + " (" + downloadedBytes.length + " bytes)");
                            break;

                        case "delete":
                            if (parts.length < 2) {
                                System.out.println("Usage: delete <filename>");
                                break;
                            }
                            String deleteFilename = normalizeDistributedFilename(parts[1]);
                            System.out.println("Deleting file: " + deleteFilename);
                            boolean deleteSuccess = storageManager.deleteFile(deleteFilename, isLeader);
                            System.out.println(deleteSuccess ? "File deleted successfully" : "Failed to delete file");
                            break;

                        case "list":
                            System.out.println("Listing all files:");
                            List<FileMetadata> files = storageManager.listFiles();

                            if (files.isEmpty()) {
                                System.out.println("No files found");
                            } else {
                                for (FileMetadata file : files) {
                                    System.out.printf("- %s (ID: %s, Size: %d bytes, Replicas: %d, Modified: %s)%n",
                                            file.getFilename(), file.getFileId(), file.getSize(),
                                            file.getLocations().size(), file.getModifiedAt());
                                }
                                System.out.println("Total: " + files.size() + " files");
                            }
                            break;

                        case "stats":
                            System.out.println("Storage statistics:");
                            Map<String, Object> stats = storageManager.getStatistics();
                            for (Map.Entry<String, Object> entry : stats.entrySet()) {
                                System.out.printf("- %s: %s%n", entry.getKey(), entry.getValue());
                            }
                            break;

                        case "status":
                            System.out.println("Node Status:");
                            System.out.println("- Server ID: " + config.getServerId());
                            System.out.println("- Role: " + (isLeader ? "LEADER" : "FOLLOWER"));
                            System.out.println("- ZooKeeper: " + config.getZookeeperAddress());
                            System.out.println("- Port: " + config.getStoragePort());
                            System.out.println("- Storage: " + storagePath + "/" + config.getServerId());
                            break;
                            
                        case "leader":
                            if (leaderElection.isLeader()) {
                                System.out.println("YES! I am the LEADER!");
                            } else {
                                System.out.println("NO. I am a follower. Leader is: " + 
                                                  leaderElection.getCurrentLeader());
                            }
                            break;
                            
                        case "servers":
                            System.out.println("Active servers: " + leaderElection.getActiveServers());
                            break;

                        case "health":
                            printHealthReport();
                            break;
                            
                        case "timesync":
                        case "clock":
                            printTimeSyncStatus();
                            break;
                            
                        case "quit":
                        case "exit":
                            System.out.println("Shutting down...");
                            running = false;
                            break;
                            
                        default:
                            System.out.println("Unknown command. Available: put, putfile, get, getfile, delete, list, stats, status, leader, servers, health, timesync, quit");
                    }
                } catch (Exception e) {
                    System.err.println("Error executing command: " + e.getMessage());
                    logger.error("Command execution error", e);
                }
            }
        }
        
        shutdown();
    }
    
    /**
     * Gracefully shut down the node.
     */
    public void shutdown() {
        try {
            // Stop time sync service first
            if (timeSyncService != null) {
                timeSyncService.stop();
                logger.info("TimeSyncService stopped for server {}", config.getServerId());
            }
            
            if (leaderElection != null) {
                leaderElection.shutdown();
            }
            if (zkConnection != null) {
                zkConnection.close();
            }
            logger.info("StorageNode {} shutdown complete", config.getServerId());
            System.out.println("Storage node " + config.getServerId() + " stopped.");
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }
    
    /**
     * Main method - entry point for each storage node.
     * Usage: java StorageNode <server-id> [zookeeper-address] [storage-port]
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java StorageNode <server-id> [zookeeper-address] [storage-port]");
            System.out.println("Example: java StorageNode server-1");
            System.out.println("Example: java StorageNode server-2 localhost:2181");
            System.out.println("Example: java StorageNode server-3 localhost:2181 8083");
            System.exit(1);
        }
        
        String serverId = args[0];
        String zkAddress = args.length >= 2 ? args[1] : "localhost:2181";
        int storagePort = args.length >= 3 ? Integer.parseInt(args[2]) : derivePort(serverId);

        ServerConfig config = new ServerConfig(serverId, zkAddress, storagePort);
        StorageNode node = new StorageNode(config);
        
        // Add shutdown hook for clean exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutdown signal received...");
            node.shutdown();
        }));
        
        try {
            node.start();
        } catch (Exception e) {
            System.err.println("Failed to start storage node: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static int derivePort(String serverId) {
        int end = serverId.length() - 1;
        while (end >= 0 && Character.isDigit(serverId.charAt(end))) {
            end--;
        }

        if (end == serverId.length() - 1) {
            return 8080;
        }

        String suffixText = serverId.substring(end + 1);
        int suffix = Integer.parseInt(suffixText);
        return suffix > 0 ? 8080 + suffix : 8080;
    }

    private static List<String> tokenizeArguments(String input) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);

            if (ch == '"') {
                inQuotes = !inQuotes;
                continue;
            }

            if (Character.isWhitespace(ch) && !inQuotes) {
                if (current.length() > 0) {
                    args.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }

            current.append(ch);
        }

        if (current.length() > 0) {
            args.add(current.toString());
        }

        return args;
    }

    private static String stripWrappingQuotes(String value) {
        String trimmed = value == null ? "" : value.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static String normalizeDistributedFilename(String raw) {
        String value = stripWrappingQuotes(raw);
        if (value.contains("/") || value.contains("\\")) {
            try {
                Path path = Paths.get(value);
                Path fileName = path.getFileName();
                if (fileName != null) {
                    value = fileName.toString();
                }
            } catch (Exception ignored) {
                // Keep original value if it cannot be parsed as a path.
            }
        }
        return value;
    }
}
