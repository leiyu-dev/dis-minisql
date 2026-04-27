package edu.minisql.distributed.coordinator;

import com.sun.net.httpserver.HttpServer;
import edu.minisql.distributed.common.HttpUtil;
import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.common.SqlUtils;
import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.protocol.ExecuteRequest;
import edu.minisql.distributed.protocol.ExecuteResponse;
import edu.minisql.distributed.protocol.NodeInfo;
import edu.minisql.distributed.protocol.ShardMetadata;
import edu.minisql.distributed.protocol.SqlRequest;
import edu.minisql.distributed.protocol.SqlResponse;
import edu.minisql.distributed.zk.ZkMetadataStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

public class CoordinatorServer {
    private final ClusterConfig config;
    private final ZkMetadataStore metadataStore;
    private final ReplicaChooser replicaChooser = new ReplicaChooser();

    public CoordinatorServer(ClusterConfig config) {
        this.config = config;
        this.metadataStore = new ZkMetadataStore(config);
        this.metadataStore.initializeShards(config);
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(config.coordinatorHost, config.coordinatorPort), 0);
        server.createContext("/sql", exchange -> {
            try {
                SqlRequest request = Jsons.parse(HttpUtil.body(exchange), SqlRequest.class);
                SqlResponse response = execute(request);
                HttpUtil.json(exchange, response.ok ? 200 : 500, response);
            } catch (Exception e) {
                HttpUtil.json(exchange, 500, SqlResponse.error(e.getMessage()));
            }
        });
        server.createContext("/metadata", exchange -> HttpUtil.json(exchange, 200, metadataStore.shards()));
        server.createContext("/nodes", exchange -> HttpUtil.json(exchange, 200, metadataStore.liveNodes()));
        server.setExecutor(Executors.newFixedThreadPool(16));
        server.start();
        System.out.printf("Coordinator serving %s at %s:%d%n",
                config.clusterName, config.coordinatorHost, config.coordinatorPort);
    }

    public SqlResponse execute(SqlRequest request) {
        String sql = SqlUtils.normalize(request.sql);
        if (sql.isBlank()) {
            return SqlResponse.error("sql is empty");
        }
        if (SqlUtils.isBroadcastDdl(sql)) {
            return broadcastWrite(sql);
        }
        if (SqlUtils.isReadOnly(sql)) {
            return distributedRead(sql, request.shardKey);
        }
        return routedWrite(sql, request.shardKey);
    }

    private SqlResponse routedWrite(String sql, String explicitShardKey) {
        String shardKey = explicitShardKey != null && !explicitShardKey.isBlank()
                ? explicitShardKey
                : SqlUtils.inferShardKey(sql);
        if (shardKey == null || shardKey.isBlank()) {
            return SqlResponse.error("No shard key provided. Put the shard key as first INSERT value or pass shardKey.");
        }
        int shardId = SqlUtils.hashToShard(shardKey, config.shardCount);
        ShardMetadata shard = metadataStore.shard(shardId);
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        List<NodeInfo> replicas = replicaChooser.chooseForWrite(shard.replicas, liveNodes);
        if (replicas.isEmpty()) {
            return SqlResponse.error("No live replica for shard " + shardId);
        }
        List<ExecuteResponse> responses = sendToReplicas(UUID.randomUUID().toString(), shardId, sql, replicas, false, 0);
        boolean success = responses.stream().anyMatch(response -> response.ok);
        return success ? SqlResponse.ok("write shard " + shardId + " replicas " + shard.replicas, responses)
                : SqlResponse.error("All replicas failed for shard " + shardId);
    }

    private SqlResponse broadcastWrite(String sql) {
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        if (liveNodes.isEmpty()) {
            return SqlResponse.error("No live nodes");
        }
        List<ExecuteResponse> responses = sendToReplicas(UUID.randomUUID().toString(), -1, sql, liveNodes, false, 0);
        boolean success = responses.stream().anyMatch(response -> response.ok);
        return success ? SqlResponse.ok("broadcast", responses) : SqlResponse.error("Broadcast failed on all nodes");
    }

    private SqlResponse distributedRead(String sql, String explicitShardKey) {
        List<ExecuteResponse> responses = new ArrayList<>();
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        if (liveNodes.isEmpty()) {
            return SqlResponse.error("No live nodes");
        }
        if (explicitShardKey != null && !explicitShardKey.isBlank()) {
            int shardId = SqlUtils.hashToShard(explicitShardKey, config.shardCount);
            ShardMetadata shard = metadataStore.shard(shardId);
            NodeInfo node = replicaChooser.chooseForRead(shardId, shard.replicas, liveNodes);
            responses.add(send(UUID.randomUUID().toString(), shardId, sql, node, false, 0));
            return SqlResponse.ok("read shard " + shardId, responses);
        }
        for (ShardMetadata shard : metadataStore.shards()) {
            NodeInfo node = replicaChooser.chooseForRead(shard.shardId, shard.replicas, liveNodes);
            responses.add(send(UUID.randomUUID().toString(), shard.shardId, sql, node, false, 0));
        }
        return SqlResponse.ok("scatter-gather read", responses);
    }

    private List<ExecuteResponse> sendToReplicas(String requestId, int shardId, String sql, List<NodeInfo> replicas, boolean replay, long walSequence) {
        List<ExecuteResponse> responses = new ArrayList<>();
        for (NodeInfo node : replicas) {
            responses.add(send(requestId, shardId, sql, node, replay, walSequence));
        }
        return responses;
    }

    private ExecuteResponse send(String requestId, int shardId, String sql, NodeInfo node, boolean replay, long walSequence) {
        try {
            ExecuteRequest request = new ExecuteRequest(requestId, shardId, sql, walSequence, replay);
            return HttpUtil.postJson(node.baseUrl() + "/execute", request, ExecuteResponse.class);
        } catch (Exception e) {
            return ExecuteResponse.error(node.nodeId, e.getMessage());
        }
    }
}
