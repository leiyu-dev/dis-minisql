package com.minisql.client;

import com.minisql.common.net.MessageCodec;
import com.minisql.common.protocol.MessageType;
import com.minisql.common.protocol.SqlRequest;
import com.minisql.common.protocol.SqlResponse;

import java.io.Closeable;
import java.net.Socket;
import java.util.List;
import java.util.UUID;

/**
 * Blocking TCP client that connects to the Coordinator and executes SQL statements.
 * Maintains a persistent connection for the lifetime of a session.
 */
public class MiniSQLClient implements Closeable {

    private final Socket socket;
    private final MessageCodec codec;
    private String currentDb = "";

    public MiniSQLClient(String host, int port) throws Exception {
        this.socket = new Socket(host, port);
        this.socket.setTcpNoDelay(true);
        this.socket.setSoTimeout(60_000);
        this.codec  = new MessageCodec(socket);
    }

    /**
     * Executes a SQL statement and returns the response.
     * The current database is sent with every request.
     */
    public SqlResponse execute(String sql) throws Exception {
        // Track USE commands locally
        String trimmed = sql.trim();
        if (trimmed.toUpperCase().startsWith("USE ")) {
            String db = trimmed.substring(4).replaceAll("[;\\s]", "").toLowerCase();
            if (!db.isEmpty()) currentDb = db;
        }

        SqlRequest req = new SqlRequest(UUID.randomUUID().toString(),
                MessageType.EXECUTE, trimmed, currentDb);
        codec.writeRequest(req);
        return codec.readResponse();
    }

    /** Sends a PING and waits for PONG to verify connectivity. */
    public boolean ping() {
        try {
            SqlRequest req = SqlRequest.ping(UUID.randomUUID().toString());
            codec.writeRequest(req);
            SqlResponse resp = codec.readResponse();
            return resp.isSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    public String getCurrentDb() { return currentDb; }

    @Override
    public void close() {
        try { codec.close(); } catch (Exception ignored) {}
        try { socket.close(); } catch (Exception ignored) {}
    }

    // ------------------------------------------------------------------
    // Pretty-print helpers
    // ------------------------------------------------------------------

    /**
     * Formats a {@link SqlResponse} as a human-readable string for console display.
     */
    public static String format(SqlResponse resp) {
        if (!resp.isSuccess()) {
            return "ERROR: " + resp.getMessage();
        }
        // If we have structured rows, render them
        if (resp.getColumns() != null && !resp.getColumns().isEmpty()) {
            return renderTable(resp.getColumns(), resp.getRows());
        }
        // Otherwise return raw output or message
        if (resp.getRawOutput() != null && !resp.getRawOutput().isEmpty()) {
            return resp.getRawOutput().trim();
        }
        if (resp.getMessage() != null) {
            return resp.getMessage();
        }
        return "Query OK, " + resp.getAffectedRows() + " row affected";
    }

    private static String renderTable(List<String> cols, List<List<String>> rows) {
        int n = cols.size();
        int[] widths = new int[n];
        for (int i = 0; i < n; i++) widths[i] = cols.get(i).length();
        if (rows != null) {
            for (List<String> row : rows) {
                for (int i = 0; i < Math.min(row.size(), n); i++) {
                    widths[i] = Math.max(widths[i], row.get(i).length());
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        String div = buildDivider(widths);
        sb.append(div).append("\n");
        sb.append(buildRow(cols, widths)).append("\n");
        sb.append(div).append("\n");
        if (rows != null) {
            for (List<String> row : rows) {
                sb.append(buildRow(row, widths)).append("\n");
            }
        }
        sb.append(div).append("\n");
        int cnt = rows != null ? rows.size() : 0;
        sb.append(cnt).append(cnt == 1 ? " row in set" : " rows in set");
        return sb.toString();
    }

    private static String buildDivider(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int w : widths) sb.append("-".repeat(w + 2)).append("+");
        return sb.toString();
    }

    private static String buildRow(List<String> cells, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String v = i < cells.size() ? cells.get(i) : "";
            sb.append(" ").append(v).append(" ".repeat(widths[i] - v.length())).append(" |");
        }
        return sb.toString();
    }
}
