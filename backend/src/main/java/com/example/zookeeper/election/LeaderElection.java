package com.example.zookeeper.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LeaderElection {
    
    private final ZooKeeper zooKeeper;
    private final String serverId;
    private boolean isLeader = false;
    private String currentLeader = null;
    
    private static final String LEADER_PATH = "/leader";
    private static final String SERVERS_PATH = "/storage-servers";
    
    public LeaderElection(ZooKeeper zooKeeper, String serverId) {
        this.zooKeeper = zooKeeper;
        this.serverId = serverId;
    }

    public void initialize() throws Exception {
        // Create parent nodes if they don't exist
        createParentNodes();
        
        // Register this server as alive
        registerServer();
        
        // Attempt to become leader
        electLeader();
    }
    
    private void createParentNodes() throws Exception {
        if (zooKeeper.exists(SERVERS_PATH, false) == null) {
            zooKeeper.create(SERVERS_PATH, 
                            "Storage Servers".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
            System.out.println("[LeaderElection] Created " + SERVERS_PATH);
        }
        
        if (zooKeeper.exists(LEADER_PATH, false) == null) {
            // Create a persistent parent, but actual leader will be ephemeral
            zooKeeper.create(LEADER_PATH, 
                            "Leader will be elected".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
            System.out.println("[LeaderElection] Created " + LEADER_PATH);
        }
    }
    
    private void registerServer() throws Exception {
        String serverPath = SERVERS_PATH + "/" + serverId;
        
        if (zooKeeper.exists(serverPath, false) == null) {
            zooKeeper.create(serverPath,
                            serverId.getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL);
            System.out.println("[LeaderElection] Server " + serverId + 
                              " registered as alive");
        }
    }
    
    public void electLeader() throws Exception {
       
        CountDownLatch electionLatch = new CountDownLatch(1);
        
        Stat leaderStat = zooKeeper.exists(LEADER_PATH + "/current", false);
        
        if (leaderStat == null) {
            try {
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
                System.out.println("[LeaderElection] Another server became leader");
                isLeader = false;
                watchForLeader(electionLatch);
            }
        } else {
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
        
        electionLatch.await(2, TimeUnit.SECONDS);
    }

    private void watchForLeader(CountDownLatch latch) throws Exception {
        String leaderNodePath = LEADER_PATH + "/current";
        ElectionWatcher watcher = new ElectionWatcher(zooKeeper, serverId, this, latch);
        Stat stat = zooKeeper.exists(leaderNodePath, watcher);
        
        if (stat != null) {
            System.out.println("[LeaderElection] Watch set on leader node");
        } else {
            System.out.println("[LeaderElection] Leader node doesn't exist, trying election again");
            electLeader();
        }
    }
    
    public boolean isLeader() {
        return isLeader;
    }
    
    public String getCurrentLeader() throws Exception {
        try {
            byte[] data = zooKeeper.getData(LEADER_PATH + "/current", false, null);
            return new String(data);
        } catch (KeeperException.NoNodeException e) {
            return null;  // No leader elected yet
        }
    }
    
    public java.util.List<String> getActiveServers() throws Exception {
        return zooKeeper.getChildren(SERVERS_PATH, false);
    }
    
    public void shutdown() throws Exception {
        String serverPath = SERVERS_PATH + "/" + serverId;
        if (zooKeeper.exists(serverPath, false) != null) {
            zooKeeper.delete(serverPath, -1);
        }
        System.out.println("[LeaderElection] Server " + serverId + " shut down");
    }
}
