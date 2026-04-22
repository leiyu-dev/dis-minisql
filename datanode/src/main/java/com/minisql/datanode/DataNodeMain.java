package com.minisql.datanode;

import com.minisql.common.model.NodeInfo;
import com.minisql.datanode.replica.ReplicaManager;
import com.minisql.datanode.zk.DataNodeZkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for a DataNode process.
 *
 * Usage:
 *   java -jar datanode.jar \
 *     --node-id=node1 \
 *     --host=localhost \
 *     --port=9001 \
 *     --zk=localhost:2181 \
 *     --minisql=./minisql/build/minisql \
 *     --data-dir=./data
 */
public class DataNodeMain {

    private static final Logger log = LoggerFactory.getLogger(DataNodeMain.class);

    public static void main(String[] args) throws Exception {
        DataNodeConfig config = DataNodeConfig.fromArgs(args);
        config.validate();

        log.info("Starting DataNode: nodeId={}, host={}, port={}",
                config.getNodeId(), config.getHost(), config.getPort());

        // ZooKeeper registration
        NodeInfo myInfo = new NodeInfo(config.getNodeId(), config.getHost(), config.getPort());
        DataNodeZkManager zkManager = new DataNodeZkManager(
                config.getZkConnectString(),
                config.getSessionTimeoutMs(),
                config.getConnectionTimeoutMs(),
                myInfo);

        ReplicaManager replicaManager = new ReplicaManager(config.getNodeId());
        zkManager.setNodeChangeListener(replicaManager::updateNodes);
        zkManager.start();

        // Seed with current live nodes
        replicaManager.updateNodes(zkManager.getLiveNodes());

        // Start TCP server (blocks)
        DataNodeServer server = new DataNodeServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down DataNode '{}'", config.getNodeId());
            server.stop();
            try { zkManager.close(); } catch (Exception ignored) {}
        }));

        server.start();
    }
}
