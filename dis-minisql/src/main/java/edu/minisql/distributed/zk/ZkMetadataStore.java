package edu.minisql.distributed.zk;

import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.protocol.NodeInfo;
import edu.minisql.distributed.protocol.ShardMetadata;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class ZkMetadataStore implements Closeable {
    private final ZooKeeper zk;
    private final ZkPaths paths;

    public ZkMetadataStore(ClusterConfig config) {
        try {
            CountDownLatch connected = new CountDownLatch(1);
            this.zk = new ZooKeeper(config.zkConnect, config.sessionTimeoutMs, event -> {
                if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            connected.await();
            this.paths = new ZkPaths(config.clusterName);
            ensurePersistent(paths.root());
            ensurePersistent(paths.nodes());
            ensurePersistent(paths.shards());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect ZooKeeper", e);
        }
    }

    public void initializeShards(ClusterConfig config) {
        List<String> nodeIds = config.nodes.stream()
                .map(node -> node.nodeId)
                .sorted()
                .collect(Collectors.toList());
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("At least one data node is required");
        }
        for (int shardId = 0; shardId < config.shardCount; shardId++) {
            List<String> replicas = new ArrayList<>();
            for (int replica = 0; replica < Math.min(config.replicationFactor, nodeIds.size()); replica++) {
                replicas.add(nodeIds.get((shardId + replica) % nodeIds.size()));
            }
            ShardMetadata metadata = new ShardMetadata(shardId, replicas, replicas.get(0));
            upsertPersistent(paths.shard(shardId), Jsons.bytes(metadata));
        }
    }

    public void registerNode(NodeInfo info) {
        upsertEphemeral(paths.node(info.nodeId), Jsons.bytes(info));
    }

    public void updateNode(NodeInfo info) {
        upsertEphemeral(paths.node(info.nodeId), Jsons.bytes(info));
    }

    public List<NodeInfo> liveNodes() {
        try {
            List<NodeInfo> result = new ArrayList<>();
            for (String child : zk.getChildren(paths.nodes(), false)) {
                byte[] data = zk.getData(paths.nodes() + "/" + child, false, null);
                result.add(Jsons.parse(data, NodeInfo.class));
            }
            result.sort(Comparator.comparing(node -> node.nodeId));
            return result;
        } catch (KeeperException.NoNodeException e) {
            return List.of();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read live nodes", e);
        }
    }

    public List<ShardMetadata> shards() {
        try {
            List<ShardMetadata> result = new ArrayList<>();
            for (String child : zk.getChildren(paths.shards(), false)) {
                byte[] data = zk.getData(paths.shards() + "/" + child, false, null);
                result.add(Jsons.parse(data, ShardMetadata.class));
            }
            result.sort(Comparator.comparingInt(shard -> shard.shardId));
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read shards", e);
        }
    }

    public ShardMetadata shard(int shardId) {
        try {
            byte[] data = zk.getData(paths.shard(shardId), false, null);
            return Jsons.parse(data, ShardMetadata.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read shard " + shardId, e);
        }
    }

    public Set<Integer> shardsForNode(String nodeId) {
        return shards().stream()
                .filter(shard -> shard.replicas.contains(nodeId))
                .map(shard -> shard.shardId)
                .collect(java.util.stream.Collectors.toSet());
    }

    private void ensurePersistent(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
            }
        }
    }

    private void upsertPersistent(String path, byte[] data) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(path, data, stat.getVersion());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    private void upsertEphemeral(String path, byte[] data) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                zk.setData(path, data, stat.getVersion());
            }
        } catch (KeeperException.NodeExistsException e) {
            try {
                zk.delete(path, -1);
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception nested) {
                throw new IllegalStateException("Failed to replace ephemeral node " + path, nested);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            zk.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
