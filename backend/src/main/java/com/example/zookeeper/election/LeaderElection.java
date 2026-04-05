package com.example.zookeeper.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Handles leader election for the distributed system.
 * One server becomes leader, others are followers.
 * If leader dies, a new leader is automatically elected.
 */
public class LeaderElection {
    
    private final ZooKeeper zooKeeper;
    private final String serverId;
    private boolean isLeader = false;
    private String currentLeader = null;
    
    // Paths in ZooKeeper
    private static final String LEADER_PATH = "/leader";
    private static final String SERVERS_PATH = "/storage-servers";
    
    public LeaderElection(ZooKeeper zooKeeper, String serverId) {
        this.zooKeeper = zooKeeper;
        this.serverId = serverId;
    }
    
    /**
     * Initialize the election process.
     * Creates necessary parent nodes and attempts to become leader.
     */
    public void initialize() throws Exception {
        // Create parent nodes if they don't exist
        createParentNodes();
        
        // Register this server as alive
        registerServer();
        
        // Attempt to become leader
        electLeader();
    }
    
    /**
     * Create necessary parent nodes in ZooKeeper.
     */
    private void createParentNodes() throws Exception {
        // Create /storage-servers parent node if it doesn't exist
        if (zooKeeper.exists(SERVERS_PATH, false) == null) {
            zooKeeper.create(SERVERS_PATH, 
                            "Storage Servers".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
            System.out.println("[LeaderElection] Created " + SERVERS_PATH);
        }
        
        // Create /leader parent node (just to ensure path exists)
        // We don't create the actual leader node here - that happens during election
        if (zooKeeper.exists(LEADER_PATH, false) == null) {
            // Create a persistent parent, but actual leader will be ephemeral
            zooKeeper.create(LEADER_PATH, 
                            "Leader will be elected".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
            System.out.println("[LeaderElection] Created " + LEADER_PATH);
        }
    }
    
    /**
     * Register this server as alive by creating an ephemeral node.
     */
    private void registerServer() throws Exception {
        String serverPath = SERVERS_PATH + "/" + serverId;
        
        // Create ephemeral node - this will disappear if server crashes
        if (zooKeeper.exists(serverPath, false) == null) {
            zooKeeper.create(serverPath,
                            serverId.getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL);
            System.out.println("[LeaderElection] Server " + serverId + 
                              " registered as alive");
        }
    }
    
    /**
     * Attempt to become the leader.
     * This is the core election logic.
     */
    public void electLeader() throws Exception {
        // Create a latch to wait for the election result
        CountDownLatch electionLatch = new CountDownLatch(1);
        
        // Check if leader already exists
        Stat leaderStat = zooKeeper.exists(LEADER_PATH + "/current", false);
        
        if (leaderStat == null) {
            // No leader exists - try to become leader
            try {
                // Create leader node (ephemeral so it disappears if we die)
                String leaderNodePath = zooKeeper.create(
                    LEADER_PATH + "/current",
                    serverId.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
                );
                
                isLeader = true;
                currentLeader = serverId;
                System.out.println("\n****************************************");
                System.out.println("[LeaderElection] I am the LEADER! " + serverId);
                System.out.println("****************************************\n");
                
                electionLatch.countDown();
                
            } catch (KeeperException.NodeExistsException e) {
                // Someone else created it between our check and create
                // Fall through to watch for leader
                System.out.println("[LeaderElection] Another server became leader");
                isLeader = false;
                watchForLeader(electionLatch);
            }
        } else {
            // Get current leader's identity

            byte[] leaderData = zooKeeper.getData(LEADER_PATH + "/current", false, null);
            currentLeader = new String(leaderData);
            if (serverId.equals(currentLeader)) {
                isLeader = true;
                System.out.println("[LeaderElection] Server " + serverId + " is the LEADER");
            } else {
                isLeader = false;
                System.out.println("[LeaderElection] Server " + serverId + 
                " is a FOLLOWER. Current leader: " + currentLeader);
            }
            watchForLeader(electionLatch);
        }
        
        // Wait a bit to ensure the watch is set
        electionLatch.await(2, TimeUnit.SECONDS);
    }
    
    /**
     * Set a watch on the leader node.
     * If leader disappears, we'll be notified.
     */
    private void watchForLeader(CountDownLatch latch) throws Exception {
        // Set watch on leader node
        String leaderNodePath = LEADER_PATH + "/current";
        
        // This watch will trigger if the leader node is deleted
        ElectionWatcher watcher = new ElectionWatcher(zooKeeper, serverId, this, latch);
        Stat stat = zooKeeper.exists(leaderNodePath, watcher);
        
        if (stat != null) {
            System.out.println("[LeaderElection] Watch set on leader node");
        } else {
            System.out.println("[LeaderElection] Leader node doesn't exist, trying election again");
            // Leader node disappeared between check and watch
            // Try to become leader
            electLeader();
        }
    }
    
    /**
     * Check if this server is the leader.
     */
    public boolean isLeader() {
        return isLeader;
    }
    
    /**
     * Get the current leader's ID.
     */
    public String getCurrentLeader() throws Exception {
        try {
            byte[] data = zooKeeper.getData(LEADER_PATH + "/current", false, null);
            return new String(data);
        } catch (KeeperException.NoNodeException e) {
            return null;  // No leader elected yet
        }
    }
    
    /**
     * Get all active servers.
     */
    public java.util.List<String> getActiveServers() throws Exception {
        return zooKeeper.getChildren(SERVERS_PATH, false);
    }
    
    /**
     * Clean up when shutting down.
     */
    public void shutdown() throws Exception {
        // Ephemeral nodes will be deleted automatically when connection closes
        // But we can explicitly remove if needed
        String serverPath = SERVERS_PATH + "/" + serverId;
        if (zooKeeper.exists(serverPath, false) != null) {
            zooKeeper.delete(serverPath, -1);
        }
        System.out.println("[LeaderElection] Server " + serverId + " shut down");
    }
}
