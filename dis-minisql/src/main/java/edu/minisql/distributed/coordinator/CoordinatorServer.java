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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CoordinatorServer {
    private final ClusterConfig config;
    private final ZkMetadataStore metadataStore;
    private final ReplicaChooser replicaChooser = new ReplicaChooser();
    private final QueryPostProcessor queryPostProcessor = new QueryPostProcessor();

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
        server.createContext("/admin/health", exchange -> HttpUtil.json(exchange, 200, clusterHealth()));
        server.createContext("/admin/rebalance", exchange -> {
            List<ShardMetadata> updated = metadataStore.rebalance(metadataStore.liveNodes(), config.replicationFactor);
            refreshAllLiveNodes();
            HttpUtil.json(exchange, 200, updated);
        });
        server.createContext("/admin/snapshot", exchange -> {
            List<NodeInfo> results = snapshotAllLiveNodes();
            HttpUtil.json(exchange, 200, results);
        });
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
            if (queryPostProcessor.isSimpleJoin(sql)) {
                return distributedJoin(sql);
            }
            String dispatchSql = queryPostProcessor.isAggregate(sql)
                    ? queryPostProcessor.baseSqlForAggregate(sql)
                    : queryPostProcessor.sqlWithoutOrderBy(sql);
            return distributedRead(sql, dispatchSql, request.shardKey);
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
        shard = metadataStore.electPrimary(shardId, liveNodes);
        List<NodeInfo> replicas = replicaChooser.chooseForWrite(shard.replicas, liveNodes);
        if (replicas.isEmpty()) {
            return SqlResponse.error("No live replica for shard " + shardId);
        }
        long shardLogIndex = metadataStore.nextShardLogIndex(shardId);
        String requestId = UUID.randomUUID().toString();
        List<ExecuteResponse> responses = sendRaftStyle(requestId, shard, shardLogIndex, sql, replicas);
        long successCount = responses.stream().filter(response -> response.ok).count();
        int quorum = quorumSize(shard.replicas.size());
        if (successCount >= quorum) {
            metadataStore.updateCommitIndex(shardId, shardLogIndex);
            return SqlResponse.ok("raft write shard " + shardId + " term " + shard.term
                    + " commitIndex " + shardLogIndex, responses);
        }
        return SqlResponse.error("Raft quorum failed for shard " + shardId + ": "
                + successCount + "/" + quorum + " acknowledgements");
    }

    private SqlResponse broadcastWrite(String sql) {
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        if (liveNodes.isEmpty()) {
            return SqlResponse.error("No live nodes");
        }
        List<ExecuteResponse> responses = sendToReplicas(UUID.randomUUID().toString(), -1, 0, 0, sql, liveNodes, false, 0);
        long successCount = responses.stream().filter(response -> response.ok).count();
        int quorum = quorumSize(liveNodes.size());
        return successCount >= quorum ? SqlResponse.ok("quorum broadcast", responses)
                : SqlResponse.error("Broadcast quorum failed: " + successCount + "/" + quorum);
    }

    private SqlResponse distributedRead(String originalSql, String dispatchSql, String explicitShardKey) {
        List<ExecuteResponse> responses = readResponses(dispatchSql, explicitShardKey);
        if (responses.isEmpty()) {
            return SqlResponse.error("No live nodes");
        }
        String merged = queryPostProcessor.mergeSelect(originalSql, responses);
        String route = explicitShardKey != null && !explicitShardKey.isBlank() ? "read single shard" : "scatter-gather read";
        return SqlResponse.ok(route, responses, merged);
    }

    private SqlResponse distributedJoin(String sql) {
        List<ExecuteResponse> leftResponses = readResponses(queryPostProcessor.leftJoinSql(sql), null);
        List<ExecuteResponse> rightResponses = readResponses(queryPostProcessor.rightJoinSql(sql), null);
        List<ExecuteResponse> allResponses = new ArrayList<>();
        allResponses.addAll(leftResponses);
        allResponses.addAll(rightResponses);
        if (allResponses.isEmpty()) {
            return SqlResponse.error("No live nodes");
        }
        String merged = queryPostProcessor.join(sql, leftResponses, rightResponses);
        return SqlResponse.ok("coordinator hash join", allResponses, merged);
    }

    private List<ExecuteResponse> readResponses(String sql, String explicitShardKey) {
        List<ExecuteResponse> responses = new ArrayList<>();
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        if (liveNodes.isEmpty()) {
            return responses;
        }
        if (explicitShardKey != null && !explicitShardKey.isBlank()) {
            int shardId = SqlUtils.hashToShard(explicitShardKey, config.shardCount);
            ShardMetadata shard = metadataStore.shard(shardId);
            NodeInfo node = replicaChooser.chooseForRead(shardId, shard.replicas, liveNodes);
            responses.add(send(UUID.randomUUID().toString(), shardId, 0, sql, node, false, 0));
            return responses;
        }
        for (ShardMetadata shard : metadataStore.shards()) {
            NodeInfo node = replicaChooser.chooseForRead(shard.shardId, shard.replicas, liveNodes);
            responses.add(send(UUID.randomUUID().toString(), shard.shardId, 0, sql, node, false, 0));
        }
        return responses;
    }

    private List<ExecuteResponse> sendToReplicas(String requestId, int shardId, long shardLogIndex, String sql,
                                                 List<NodeInfo> replicas, boolean replay, long walSequence) {
        return sendToReplicas(requestId, shardId, shardLogIndex, 0, sql, replicas, replay, walSequence);
    }

    private List<ExecuteResponse> sendToReplicas(String requestId, int shardId, long shardLogIndex, long raftTerm,
                                                 String sql, List<NodeInfo> replicas, boolean replay, long walSequence) {
        List<ExecuteResponse> responses = new ArrayList<>();
        for (NodeInfo node : replicas) {
            responses.add(send(requestId, shardId, shardLogIndex, raftTerm, sql, node, replay, walSequence));
        }
        return responses;
    }

    private List<ExecuteResponse> sendRaftStyle(String requestId, ShardMetadata shard, long shardLogIndex,
                                                String sql, List<NodeInfo> replicas) {
        Map<String, NodeInfo> byId = replicas.stream().collect(Collectors.toMap(node -> node.nodeId, node -> node));
        List<ExecuteResponse> responses = new ArrayList<>();
        NodeInfo primary = byId.get(shard.primary);
        if (primary != null) {
            responses.add(send(requestId, shard.shardId, shardLogIndex, shard.term, sql, primary, false, 0));
        }
        for (NodeInfo replica : replicas) {
            if (!replica.nodeId.equals(shard.primary)) {
                responses.add(send(requestId, shard.shardId, shardLogIndex, shard.term, sql, replica, true, 0));
            }
        }
        return responses;
    }

    private ExecuteResponse send(String requestId, int shardId, long shardLogIndex, String sql, NodeInfo node,
                                 boolean replay, long walSequence) {
        return send(requestId, shardId, shardLogIndex, 0, sql, node, replay, walSequence);
    }

    private ExecuteResponse send(String requestId, int shardId, long shardLogIndex, long raftTerm, String sql,
                                 NodeInfo node, boolean replay, long walSequence) {
        try {
            ExecuteRequest request = new ExecuteRequest(requestId, shardId, shardLogIndex, raftTerm, sql, walSequence, replay);
            return HttpUtil.postJson(node.baseUrl() + "/execute", request, ExecuteResponse.class);
        } catch (Exception e) {
            return ExecuteResponse.error(node.nodeId, e.getMessage());
        }
    }

    private int quorumSize(int replicaCount) {
        return replicaCount / 2 + 1;
    }

    private List<NodeInfo> refreshAllLiveNodes() {
        List<NodeInfo> results = new ArrayList<>();
        for (NodeInfo node : metadataStore.liveNodes()) {
            try {
                results.add(HttpUtil.postJson(node.baseUrl() + "/admin/refresh-shards", Map.of(), NodeInfo.class));
            } catch (Exception ignored) {
            }
        }
        return results;
    }

    private List<NodeInfo> snapshotAllLiveNodes() {
        List<NodeInfo> results = new ArrayList<>();
        for (NodeInfo node : metadataStore.liveNodes()) {
            try {
                HttpUtil.postJson(node.baseUrl() + "/admin/snapshot", Map.of(), Object.class);
                results.add(node);
            } catch (Exception ignored) {
            }
        }
        return results;
    }

    private Map<String, Object> clusterHealth() {
        return Map.of(
                "nodes", metadataStore.liveNodes(),
                "shards", metadataStore.shards()
        );
    }
}
