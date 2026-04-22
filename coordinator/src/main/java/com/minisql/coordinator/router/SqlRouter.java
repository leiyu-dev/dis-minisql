package com.minisql.coordinator.router;

import com.minisql.common.model.NodeInfo;
import com.minisql.common.model.ShardInfo;
import com.minisql.common.model.TableMeta;
import com.minisql.common.net.MessageCodec;
import com.minisql.common.protocol.MessageType;
import com.minisql.common.protocol.SqlRequest;
import com.minisql.common.protocol.SqlResponse;
import com.minisql.coordinator.meta.ClusterManager;
import com.minisql.coordinator.meta.MetadataManager;
import com.minisql.coordinator.shard.ShardingStrategy;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * Routes SQL statements to the appropriate DataNode(s), collects responses,
 * and merges them into a single {@link SqlResponse} for the client.
 *
 * Routing rules:
 *   DDL (CREATE/DROP DATABASE|TABLE|INDEX):
 *     → broadcast to all live nodes (schema must be consistent everywhere)
 *   INSERT:
 *     → route to the shard determined by hash(pk_value) % numShards
 *     → then replicate to that shard's replicas
 *   SELECT / UPDATE / DELETE with shard-key equality in WHERE:
 *     → route to the single matching shard
 *   SELECT / UPDATE / DELETE without shard-key equality:
 *     → fan-out to all shards, merge results
 *   SELECT with JOIN:
 *     → handled by QueryPlanner (pull both tables, join in memory)
 *   SHOW DATABASES / SHOW TABLES:
 *     → answered directly from ZooKeeper metadata (no DataNode call)
 */
public class SqlRouter {

    private static final Logger log = LoggerFactory.getLogger(SqlRouter.class);

    private final MetadataManager metadataManager;
    private final ClusterManager clusterManager;
    private final ResultMerger merger;
    private final ExecutorService fanOutPool;

    public SqlRouter(MetadataManager metadataManager, ClusterManager clusterManager) {
        this.metadataManager = metadataManager;
        this.clusterManager  = clusterManager;
        this.merger          = new ResultMerger();
        this.fanOutPool      = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "fanout-worker");
            t.setDaemon(true);
            return t;
        });
    }

    // ------------------------------------------------------------------
    // Main entry point
    // ------------------------------------------------------------------

    public SqlResponse route(String requestId, String database, String sql) {
        String trimmed = sql.trim();
        String upperTrimmed = trimmed.toUpperCase();

        try {
            // ---- Special commands handled without DataNode ----
            if (upperTrimmed.startsWith("SHOW DATABASES")) {
                return handleShowDatabases(requestId);
            }
            if (upperTrimmed.startsWith("SHOW TABLES")) {
                return handleShowTables(requestId, database);
            }
            if (upperTrimmed.startsWith("CREATE DATABASE")) {
                return handleCreateDatabase(requestId, database, trimmed);
            }
            if (upperTrimmed.startsWith("DROP DATABASE")) {
                return handleDropDatabase(requestId, trimmed);
            }
            if (upperTrimmed.startsWith("USE ")) {
                // "USE dbname" — broadcast so every node switches too;
                // the logical db is tracked in the session (ClientHandler)
                return broadcastAndMerge(requestId, database, trimmed);
            }

            // ---- Parse with JSQLParser ----
            Statement stmt;
            try {
                stmt = CCJSqlParserUtil.parse(trimmed);
            } catch (Exception e) {
                // Fall back: broadcast raw SQL to all nodes
                log.warn("Cannot parse SQL, broadcasting: {}", trimmed);
                return broadcastAndMerge(requestId, database, trimmed);
            }

            if (stmt instanceof net.sf.jsqlparser.statement.create.table.CreateTable) {
                return handleCreateTable(requestId, database, trimmed);
            }
            if (stmt instanceof Drop) {
                return handleDrop(requestId, database, trimmed, (Drop) stmt);
            }
            if (stmt instanceof net.sf.jsqlparser.statement.create.index.CreateIndex) {
                return broadcastAndMerge(requestId, database, trimmed);
            }
            if (stmt instanceof Insert) {
                return handleInsert(requestId, database, trimmed, (Insert) stmt);
            }
            if (stmt instanceof Select) {
                return handleSelect(requestId, database, trimmed, (Select) stmt);
            }
            if (stmt instanceof Update) {
                return handleUpdate(requestId, database, trimmed, (Update) stmt);
            }
            if (stmt instanceof Delete) {
                return handleDelete(requestId, database, trimmed, (Delete) stmt);
            }

            // Unknown: broadcast
            return broadcastAndMerge(requestId, database, trimmed);

        } catch (Exception e) {
            log.error("Routing error for sql=[{}]", sql, e);
            return SqlResponse.error(requestId, "Routing error: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // DDL handlers
    // ------------------------------------------------------------------

    private SqlResponse handleCreateDatabase(String requestId, String database, String sql) throws Exception {
        // Parse db name from "CREATE DATABASE dbname"
        String[] tokens = sql.split("\\s+");
        String dbName = tokens.length >= 3 ? tokens[2].replaceAll("[;`'\"]", "") : database;
        SqlResponse resp = broadcastAndMerge(requestId, "", sql);
        if (resp.isSuccess()) {
            metadataManager.registerDatabase(dbName);
        }
        return resp;
    }

    private SqlResponse handleDropDatabase(String requestId, String sql) throws Exception {
        String[] tokens = sql.split("\\s+");
        String dbName = tokens.length >= 3 ? tokens[2].replaceAll("[;`'\"]", "") : "";
        SqlResponse resp = broadcastAndMerge(requestId, "", sql);
        if (resp.isSuccess() && !dbName.isEmpty()) {
            metadataManager.deregisterDatabase(dbName);
        }
        return resp;
    }

    private SqlResponse handleCreateTable(String requestId, String database, String sql) throws Exception {
        SqlResponse resp = broadcastAndMerge(requestId, database, sql);
        if (resp.isSuccess() && database != null && !database.isEmpty()) {
            try {
                metadataManager.registerTable(database, sql);
            } catch (Exception e) {
                log.warn("Failed to store table metadata: {}", e.getMessage());
            }
        }
        return resp;
    }

    private SqlResponse handleDrop(String requestId, String database, String sql, Drop drop) throws Exception {
        SqlResponse resp = broadcastAndMerge(requestId, database, sql);
        if (resp.isSuccess() && "TABLE".equalsIgnoreCase(drop.getType())) {
            try {
                metadataManager.deregisterTable(database, drop.getName().getName().toLowerCase());
            } catch (Exception e) {
                log.warn("Failed to remove table metadata: {}", e.getMessage());
            }
        }
        return resp;
    }

    // ------------------------------------------------------------------
    // INSERT
    // ------------------------------------------------------------------

    private SqlResponse handleInsert(String requestId, String database, String sql, Insert insert) {
        String tableName = insert.getTable().getName().toLowerCase();
        TableMeta meta   = metadataManager.getTableMeta(database, tableName);

        if (meta == null || clusterManager.getLiveNodeCount() == 0) {
            return broadcastAndMerge(requestId, database, sql);
        }

        // Extract shard key value from VALUES clause
        String shardKeyValue = extractInsertKeyValue(insert, meta);
        if (shardKeyValue == null) {
            // Cannot determine shard; send to node 0
            shardKeyValue = "0";
        }

        int shardId = ShardingStrategy.getShardId(shardKeyValue, meta.getNumShards());
        ShardInfo shard = getOrFallback(database, tableName, shardId, meta.getNumShards());
        if (shard == null) {
            return SqlResponse.error(requestId, "No shard found for " + tableName);
        }

        // Write to primary
        SqlResponse resp = sendToNode(requestId, database, sql, shard.getPrimaryNodeId());
        if (!resp.isSuccess()) return resp;

        // Replicate to replicas (async, best-effort)
        replicateAsync(database, sql, shard.getReplicaNodeIds());
        return resp;
    }

    // ------------------------------------------------------------------
    // SELECT
    // ------------------------------------------------------------------

    private SqlResponse handleSelect(String requestId, String database, String sql, Select select) {
        // Detect JOIN
        PlainSelect plain = extractPlainSelect(select);
        if (plain != null && plain.getJoins() != null && !plain.getJoins().isEmpty()) {
            return handleJoin(requestId, database, sql, plain);
        }

        // Single-table SELECT
        if (plain != null && plain.getFromItem() instanceof net.sf.jsqlparser.schema.Table) {
            String tableName = ((net.sf.jsqlparser.schema.Table) plain.getFromItem()).getName().toLowerCase();
            TableMeta meta = metadataManager.getTableMeta(database, tableName);
            if (meta != null) {
                String shardKey = extractWhereEquality(plain.getWhere(), meta.getShardKey());
                if (shardKey != null) {
                    // Route to specific shard (load-balance reads to any replica)
                    int shardId = ShardingStrategy.getShardId(shardKey, meta.getNumShards());
                    ShardInfo shard = getOrFallback(database, tableName, shardId, meta.getNumShards());
                    if (shard != null) {
                        String readNode = pickReadNode(shard);
                        return sendToNode(requestId, database, sql, readNode);
                    }
                }
            }
        }

        // Fan-out to all nodes
        return fanOutAndMergeRows(requestId, database, sql);
    }

    // ------------------------------------------------------------------
    // JOIN (in-memory hash join at coordinator)
    // ------------------------------------------------------------------

    private SqlResponse handleJoin(String requestId, String database, String sql, PlainSelect plain) {
        try {
            // Left table
            if (!(plain.getFromItem() instanceof net.sf.jsqlparser.schema.Table)) {
                return fanOutAndMergeRows(requestId, database, sql);
            }
            String leftTable  = ((net.sf.jsqlparser.schema.Table) plain.getFromItem()).getName().toLowerCase();
            String rightTable = null;
            String leftJoinCol = null, rightJoinCol = null;

            // We only handle the first JOIN for now
            Join join = plain.getJoins().get(0);
            if (join.getRightItem() instanceof net.sf.jsqlparser.schema.Table) {
                rightTable = ((net.sf.jsqlparser.schema.Table) join.getRightItem()).getName().toLowerCase();
            }
            // Parse ON condition
            Expression onExpr = join.getOnExpression();
            if (onExpr instanceof EqualsTo) {
                EqualsTo eq = (EqualsTo) onExpr;
                if (eq.getLeftExpression() instanceof Column && eq.getRightExpression() instanceof Column) {
                    leftJoinCol  = ((Column) eq.getLeftExpression()).getColumnName();
                    rightJoinCol = ((Column) eq.getRightExpression()).getColumnName();
                }
            }

            if (rightTable == null || leftJoinCol == null) {
                return fanOutAndMergeRows(requestId, database, sql);
            }

            // Fetch all rows of left table
            String leftSql  = "SELECT * FROM " + leftTable;
            String rightSql = "SELECT * FROM " + rightTable;

            SqlResponse leftResp  = fanOutAndMergeRows(requestId + "-L", database, leftSql);
            SqlResponse rightResp = fanOutAndMergeRows(requestId + "-R", database, rightSql);

            if (!leftResp.isSuccess())  return leftResp;
            if (!rightResp.isSuccess()) return rightResp;

            ResultMerger.JoinResult jr = merger.hashJoin(
                    leftResp.getColumns(),  nvl(leftResp.getRows()),
                    rightResp.getColumns(), nvl(rightResp.getRows()),
                    leftJoinCol, rightJoinCol);

            // Apply projection / WHERE filters by delegating back to a node with a subquery
            // (simplified: return all joined columns)
            String raw = buildJoinRaw(jr.getColumns(), jr.getRows());
            return SqlResponse.success(requestId, raw, jr.getColumns(), jr.getRows(), jr.getRows().size());

        } catch (Exception e) {
            log.error("JOIN handling error", e);
            return fanOutAndMergeRows(requestId, database, sql);
        }
    }

    // ------------------------------------------------------------------
    // UPDATE / DELETE
    // ------------------------------------------------------------------

    private SqlResponse handleUpdate(String requestId, String database, String sql, Update update) {
        String tableName = update.getTable().getName().toLowerCase();
        TableMeta meta = metadataManager.getTableMeta(database, tableName);

        if (meta != null) {
            String keyVal = extractWhereEquality(update.getWhere(), meta.getShardKey());
            if (keyVal != null) {
                int shardId = ShardingStrategy.getShardId(keyVal, meta.getNumShards());
                ShardInfo shard = getOrFallback(database, tableName, shardId, meta.getNumShards());
                if (shard != null) {
                    SqlResponse resp = sendToNode(requestId, database, sql, shard.getPrimaryNodeId());
                    if (resp.isSuccess()) replicateAsync(database, sql, shard.getReplicaNodeIds());
                    return resp;
                }
            }
        }
        return fanOutAndMergeAffected(requestId, database, sql);
    }

    private SqlResponse handleDelete(String requestId, String database, String sql, Delete delete) {
        String tableName = delete.getTable().getName().toLowerCase();
        TableMeta meta = metadataManager.getTableMeta(database, tableName);

        if (meta != null) {
            String keyVal = extractWhereEquality(delete.getWhere(), meta.getShardKey());
            if (keyVal != null) {
                int shardId = ShardingStrategy.getShardId(keyVal, meta.getNumShards());
                ShardInfo shard = getOrFallback(database, tableName, shardId, meta.getNumShards());
                if (shard != null) {
                    SqlResponse resp = sendToNode(requestId, database, sql, shard.getPrimaryNodeId());
                    if (resp.isSuccess()) replicateAsync(database, sql, shard.getReplicaNodeIds());
                    return resp;
                }
            }
        }
        return fanOutAndMergeAffected(requestId, database, sql);
    }

    // ------------------------------------------------------------------
    // SHOW helpers (answered from ZK metadata)
    // ------------------------------------------------------------------

    private SqlResponse handleShowDatabases(String requestId) {
        try {
            List<String> dbs = metadataManager.listDatabases();
            List<List<String>> rows = new ArrayList<>();
            for (String db : dbs) rows.add(Collections.singletonList(db));
            String raw = buildSimpleTable("Database", dbs);
            return SqlResponse.success(requestId, raw,
                    Collections.singletonList("Database"), rows, rows.size());
        } catch (Exception e) {
            return SqlResponse.error(requestId, e.getMessage());
        }
    }

    private SqlResponse handleShowTables(String requestId, String database) {
        try {
            if (database == null || database.isEmpty()) {
                return SqlResponse.error(requestId, "No database selected");
            }
            List<String> tables = metadataManager.listTables(database);
            List<List<String>> rows = new ArrayList<>();
            for (String t : tables) rows.add(Collections.singletonList(t));
            String raw = buildSimpleTable("Tables_in_" + database, tables);
            return SqlResponse.success(requestId, raw,
                    Collections.singletonList("Tables_in_" + database), rows, rows.size());
        } catch (Exception e) {
            return SqlResponse.error(requestId, e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Network helpers
    // ------------------------------------------------------------------

    /** Send a request to a single named data node. */
    SqlResponse sendToNode(String requestId, String database, String sql, String nodeId) {
        NodeInfo node = clusterManager.getNode(nodeId);
        if (node == null) {
            return SqlResponse.error(requestId, "Node not available: " + nodeId);
        }
        return sendToAddress(requestId, database, sql, node.getHost(), node.getPort(),
                MessageType.EXECUTE);
    }

    private SqlResponse sendToAddress(String requestId, String database, String sql,
                                       String host, int port, MessageType type) {
        try (Socket socket = new Socket(host, port);
             MessageCodec codec  = new MessageCodec(socket)) {
            socket.setSoTimeout(30_000);
            SqlRequest req = new SqlRequest(requestId, type, sql, database);
            codec.writeRequest(req);
            return codec.readResponse();
        } catch (Exception e) {
            log.error("Failed to contact node {}:{}: {}", host, port, e.getMessage());
            return SqlResponse.error(requestId, "Node unreachable: " + host + ":" + port);
        }
    }

    /** Broadcast to all live nodes; merge row results. */
    private SqlResponse broadcastAndMerge(String requestId, String database, String sql) {
        List<NodeInfo> nodes = clusterManager.getLiveNodes();
        if (nodes.isEmpty()) {
            return SqlResponse.error(requestId, "No live data nodes available");
        }
        // For DDL / USE, first successful response is fine
        SqlResponse first = null;
        for (NodeInfo node : nodes) {
            SqlResponse r = sendToAddress(requestId, database, sql,
                    node.getHost(), node.getPort(), MessageType.EXECUTE);
            if (first == null) first = r;
        }
        return first != null ? first : SqlResponse.error(requestId, "No response");
    }

    /** Fan-out SELECT to all live nodes; merge rows. */
    private SqlResponse fanOutAndMergeRows(String requestId, String database, String sql) {
        List<NodeInfo> nodes = clusterManager.getLiveNodes();
        if (nodes.isEmpty()) {
            return SqlResponse.error(requestId, "No live data nodes available");
        }
        List<Future<SqlResponse>> futures = new ArrayList<>();
        for (NodeInfo node : nodes) {
            futures.add(fanOutPool.submit(() ->
                    sendToAddress(requestId, database, sql,
                            node.getHost(), node.getPort(), MessageType.EXECUTE)));
        }
        List<SqlResponse> results = collectFutures(futures, requestId);
        return merger.mergeRows(requestId, results);
    }

    /** Fan-out UPDATE/DELETE to all live nodes; merge affected counts. */
    private SqlResponse fanOutAndMergeAffected(String requestId, String database, String sql) {
        List<NodeInfo> nodes = clusterManager.getLiveNodes();
        if (nodes.isEmpty()) {
            return SqlResponse.error(requestId, "No live data nodes available");
        }
        List<Future<SqlResponse>> futures = new ArrayList<>();
        for (NodeInfo node : nodes) {
            futures.add(fanOutPool.submit(() ->
                    sendToAddress(requestId, database, sql,
                            node.getHost(), node.getPort(), MessageType.EXECUTE)));
        }
        List<SqlResponse> results = collectFutures(futures, requestId);
        return merger.mergeAffected(requestId, results);
    }

    /** Asynchronously replicate a write to replica nodes. */
    private void replicateAsync(String database, String sql, List<String> replicaIds) {
        if (replicaIds == null) return;
        for (String rid : replicaIds) {
            NodeInfo node = clusterManager.getNode(rid);
            if (node != null) {
                fanOutPool.submit(() ->
                        sendToAddress("repl-" + System.nanoTime(), database, sql,
                                node.getHost(), node.getPort(), MessageType.REPLICATE));
            }
        }
    }

    private List<SqlResponse> collectFutures(List<Future<SqlResponse>> futures, String requestId) {
        List<SqlResponse> results = new ArrayList<>();
        for (Future<SqlResponse> f : futures) {
            try {
                results.add(f.get(30, TimeUnit.SECONDS));
            } catch (Exception e) {
                results.add(SqlResponse.error(requestId, e.getMessage()));
            }
        }
        return results;
    }

    // ------------------------------------------------------------------
    // SQL parsing helpers
    // ------------------------------------------------------------------

    private String extractInsertKeyValue(Insert insert, TableMeta meta) {
        try {
            net.sf.jsqlparser.expression.operators.relational.ExpressionList<Column> cols = insert.getColumns();
            Values values = insert.getValues();
            if (values == null) {
                return null;
            }
            net.sf.jsqlparser.expression.operators.relational.ExpressionList<?> valueRow =
                    values.getExpressions();
            if (valueRow == null) {
                return null;
            }
            List<? extends Expression> exprs = valueRow.getExpressions();
            if (cols == null || cols.isEmpty()) {
                // Positional: shard key is at meta.getShardKeyIndex()
                int idx = meta.getShardKeyIndex();
                if (idx < exprs.size()) {
                    return exprToString(exprs.get(idx));
                }
            } else {
                // Named columns
                for (int i = 0; i < cols.size(); i++) {
                    if (cols.get(i).getColumnName().equalsIgnoreCase(meta.getShardKey())) {
                        if (i < exprs.size()) {
                            return exprToString(exprs.get(i));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Could not extract insert key value: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Walks a WHERE expression tree looking for "column = literal" where
     * column name matches {@code shardKey}. Returns the literal value, or null.
     */
    String extractWhereEquality(Expression where, String shardKey) {
        if (where == null || shardKey == null) return null;
        if (where instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo) where;
            Expression left  = eq.getLeftExpression();
            Expression right = eq.getRightExpression();
            if (left instanceof Column && ((Column) left).getColumnName().equalsIgnoreCase(shardKey)) {
                return exprToString(right);
            }
            if (right instanceof Column && ((Column) right).getColumnName().equalsIgnoreCase(shardKey)) {
                return exprToString(left);
            }
        }
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            String l = extractWhereEquality(and.getLeftExpression(), shardKey);
            if (l != null) return l;
            return extractWhereEquality(and.getRightExpression(), shardKey);
        }
        return null;
    }

    private String exprToString(Expression expr) {
        if (expr instanceof LongValue)   return String.valueOf(((LongValue) expr).getValue());
        if (expr instanceof StringValue) return ((StringValue) expr).getValue();
        return expr.toString().replaceAll("'", "");
    }

    private PlainSelect extractPlainSelect(Select select) {
        return select instanceof PlainSelect ? (PlainSelect) select : null;
    }

    // ------------------------------------------------------------------
    // Shard resolution helpers
    // ------------------------------------------------------------------

    /** Returns shard info, or picks any live shard as fallback. */
    private ShardInfo getOrFallback(String db, String table, int shardId, int numShards) {
        for (int attempt = 0; attempt < numShards; attempt++) {
            int id = (shardId + attempt) % numShards;
            ShardInfo shard = metadataManager.getTableMeta(db, table) != null
                    ? metadataManager.getAllShards(db, table).stream()
                            .filter(s -> s.getShardId() == id).findFirst().orElse(null)
                    : null;
            if (shard != null && clusterManager.isAlive(shard.getPrimaryNodeId())) {
                return shard;
            }
            // If primary is dead but a replica is alive, use first live replica
            if (shard != null && shard.getReplicaNodeIds() != null) {
                for (String rid : shard.getReplicaNodeIds()) {
                    if (clusterManager.isAlive(rid)) {
                        // Temporarily treat replica as primary for routing
                        ShardInfo promoted = new ShardInfo(shard.getShardId(), db, table,
                                rid, Collections.emptyList());
                        return promoted;
                    }
                }
            }
        }
        // Last resort: round-robin among live nodes
        List<NodeInfo> live = clusterManager.getLiveNodes();
        if (!live.isEmpty()) {
            NodeInfo node = live.get(shardId % live.size());
            return new ShardInfo(shardId, db, table, node.getNodeId(), Collections.emptyList());
        }
        return null;
    }

    private String pickReadNode(ShardInfo shard) {
        // Round-robin reads across primary + replicas for load balancing
        List<String> candidates = new ArrayList<>();
        candidates.add(shard.getPrimaryNodeId());
        if (shard.getReplicaNodeIds() != null) candidates.addAll(shard.getReplicaNodeIds());
        long idx = System.nanoTime() % candidates.size();
        for (int i = 0; i < candidates.size(); i++) {
            String id = candidates.get((int)((idx + i) % candidates.size()));
            if (clusterManager.isAlive(id)) return id;
        }
        return shard.getPrimaryNodeId();
    }

    // ------------------------------------------------------------------
    // Display helpers
    // ------------------------------------------------------------------

    private String buildSimpleTable(String header, List<String> values) {
        if (values.isEmpty()) return "Empty set";
        int w = header.length();
        for (String v : values) w = Math.max(w, v.length());
        String div = "+-" + "-".repeat(w) + "-+";
        StringBuilder sb = new StringBuilder();
        sb.append(div).append("\n");
        sb.append("| ").append(padRight(header, w)).append(" |").append("\n");
        sb.append(div).append("\n");
        for (String v : values) sb.append("| ").append(padRight(v, w)).append(" |").append("\n");
        sb.append(div).append("\n");
        sb.append(values.size()).append(" row in set");
        return sb.toString();
    }

    private String buildJoinRaw(List<String> cols, List<List<String>> rows) {
        if (rows.isEmpty()) return "Empty set";
        int[] widths = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) widths[i] = cols.get(i).length();
        for (List<String> row : rows) {
            for (int i = 0; i < Math.min(row.size(), widths.length); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(divider(widths)).append("\n");
        sb.append(rowLine(cols, widths)).append("\n");
        sb.append(divider(widths)).append("\n");
        for (List<String> row : rows) sb.append(rowLine(row, widths)).append("\n");
        sb.append(divider(widths)).append("\n");
        sb.append(rows.size()).append(" row in set");
        return sb.toString();
    }

    private String divider(int[] w) {
        StringBuilder sb = new StringBuilder("+");
        for (int v : w) sb.append("-".repeat(v + 2)).append("+");
        return sb.toString();
    }

    private String rowLine(List<String> cells, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String v = i < cells.size() ? cells.get(i) : "";
            sb.append(" ").append(padRight(v, widths[i])).append(" |");
        }
        return sb.toString();
    }

    private String padRight(String s, int len) {
        if (s.length() >= len) return s;
        return s + " ".repeat(len - s.length());
    }

    private <T> List<T> nvl(List<T> list) {
        return list != null ? list : Collections.emptyList();
    }
}
