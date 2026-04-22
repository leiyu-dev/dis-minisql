package com.minisql.datanode.zk;

import com.minisql.common.model.NodeInfo;
import com.minisql.common.util.JsonUtil;
import com.minisql.common.zk.ZkPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Handles ZooKeeper registration and peer-node discovery for a DataNode.
 *
 * Each data node publishes an ephemeral ZNode at:
 *   /dis-minisql/nodes/{nodeId}
 * The ZNode value is JSON-serialised {@link NodeInfo}.
 */
public class DataNodeZkManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DataNodeZkManager.class);

    private final CuratorFramework client;
    private final NodeInfo myInfo;
    private CuratorCache nodeCache;
    private Consumer<List<NodeInfo>> nodeChangeListener;

    public DataNodeZkManager(String zkConnectString, int sessionMs, int connMs, NodeInfo myInfo) {
        this.myInfo = myInfo;
        this.client = CuratorFrameworkFactory.builder()
                .connectString(zkConnectString)
                .sessionTimeoutMs(sessionMs)
                .connectionTimeoutMs(connMs)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }

    public void start() throws Exception {
        client.start();
        client.blockUntilConnected();
        log.info("Connected to ZooKeeper");

        // Ensure base paths exist
        ensurePersistent(ZkPaths.ROOT);
        ensurePersistent(ZkPaths.NODES);
        ensurePersistent(ZkPaths.DATABASES);
        ensurePersistent(ZkPaths.TABLES);
        ensurePersistent(ZkPaths.SHARDS);

        // Register this node as ephemeral (auto-deleted on disconnect)
        String nodePath = ZkPaths.node(myInfo.getNodeId());
        byte[] data = JsonUtil.toBytes(myInfo);
        if (client.checkExists().forPath(nodePath) != null) {
            client.delete().forPath(nodePath);
        }
        client.create()
              .withMode(CreateMode.EPHEMERAL)
              .forPath(nodePath, data);
        log.info("Registered DataNode at {}", nodePath);

        // Watch peer nodes
        startNodeWatch();
    }

    public void setNodeChangeListener(Consumer<List<NodeInfo>> listener) {
        this.nodeChangeListener = listener;
    }

    private void startNodeWatch() {
        nodeCache = CuratorCache.build(client, ZkPaths.NODES);
        nodeCache.listenable().addListener(
                CuratorCacheListener.builder()
                        .forCreates(node -> notifyChange())
                        .forDeletes(node -> notifyChange())
                        .forChanges((old, cur) -> notifyChange())
                        .build());
        nodeCache.start();
    }

    private void notifyChange() {
        if (nodeChangeListener != null) {
            nodeChangeListener.accept(getLiveNodes());
        }
    }

    /** Returns a snapshot of currently registered (live) data nodes. */
    public List<NodeInfo> getLiveNodes() {
        List<NodeInfo> nodes = new ArrayList<>();
        try {
            List<String> children = client.getChildren().forPath(ZkPaths.NODES);
            for (String child : children) {
                byte[] data = client.getData().forPath(ZkPaths.NODES + "/" + child);
                if (data != null && data.length > 0) {
                    nodes.add(JsonUtil.fromBytes(data, NodeInfo.class));
                }
            }
        } catch (Exception e) {
            log.error("Failed to read live nodes from ZooKeeper", e);
        }
        return nodes;
    }

    private void ensurePersistent(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT)
                  .forPath(path);
        }
    }

    @Override
    public void close() {
        if (nodeCache != null) nodeCache.close();
        client.close();
    }
}
