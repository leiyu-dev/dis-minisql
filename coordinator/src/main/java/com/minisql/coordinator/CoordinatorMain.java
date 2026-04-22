package com.minisql.coordinator;

import com.minisql.coordinator.meta.ClusterManager;
import com.minisql.coordinator.meta.MetadataManager;
import com.minisql.coordinator.router.SqlRouter;
import com.minisql.coordinator.zk.CoordinatorZkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Coordinator process.
 *
 * Usage:
 *   java -jar coordinator.jar \
 *     --host=0.0.0.0 \
 *     --port=8080 \
 *     --zk=localhost:2181 \
 *     --replication-factor=2
 */
public class CoordinatorMain {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorMain.class);

    public static void main(String[] args) throws Exception {
        CoordinatorConfig config = CoordinatorConfig.fromArgs(args);
        config.validate();

        log.info("Starting Coordinator on port {}", config.getPort());

        // ZooKeeper
        CoordinatorZkManager zkManager = new CoordinatorZkManager(
                config.getZkConnectString(),
                config.getSessionTimeoutMs(),
                config.getConnectionTimeoutMs());
        zkManager.start();

        // Cluster manager tracks live nodes and handles failover
        ClusterManager clusterManager = new ClusterManager(zkManager);
        zkManager.setNodeChangeListener(clusterManager::updateNodes);
        clusterManager.updateNodes(zkManager.getLiveNodes());

        // Metadata manager stores table/shard info in ZK
        MetadataManager metadataManager = new MetadataManager(
                zkManager, clusterManager, config.getReplicationFactor());

        // SQL router
        SqlRouter router = new SqlRouter(metadataManager, clusterManager);

        // TCP server (blocks)
        CoordinatorServer server = new CoordinatorServer(config.getPort(), router);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Coordinator");
            server.stop();
            try { zkManager.close(); } catch (Exception ignored) {}
        }));

        server.start();
    }
}
