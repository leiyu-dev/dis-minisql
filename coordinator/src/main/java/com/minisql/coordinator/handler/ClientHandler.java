package com.minisql.coordinator.handler;

import com.minisql.common.net.MessageCodec;
import com.minisql.common.protocol.MessageType;
import com.minisql.common.protocol.SqlRequest;
import com.minisql.common.protocol.SqlResponse;
import com.minisql.coordinator.router.SqlRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.UUID;

/**
 * Handles one client connection to the Coordinator.
 * Maintains per-connection session state (current database).
 */
public class ClientHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    private final Socket socket;
    private final SqlRouter router;
    /** Current database context for this session. */
    private String currentDb = "";

    public ClientHandler(Socket socket, SqlRouter router) {
        this.socket = socket;
        this.router = router;
    }

    @Override
    public void run() {
        String remote = socket.getRemoteSocketAddress().toString();
        log.info("Client connected: {}", remote);
        try (MessageCodec codec = new MessageCodec(socket)) {
            while (!socket.isClosed()) {
                SqlRequest req = codec.readRequest();
                SqlResponse resp = handleRequest(req);
                codec.writeResponse(resp);
            }
        } catch (java.io.EOFException e) {
            log.info("Client disconnected: {}", remote);
        } catch (Exception e) {
            log.error("Error handling client {}: {}", remote, e.getMessage());
        }
    }

    private SqlResponse handleRequest(SqlRequest req) {
        if (req.getType() == MessageType.PING) {
            return SqlResponse.pong(req.getRequestId());
        }

        // Update current database if the request carries one
        if (req.getDatabase() != null && !req.getDatabase().isEmpty()) {
            currentDb = req.getDatabase();
        }

        // Detect "USE dbname" to update session database
        String sql = req.getSql().trim();
        if (sql.toUpperCase().startsWith("USE ")) {
            String dbName = sql.substring(4).replaceAll("[;\\s]", "").toLowerCase();
            if (!dbName.isEmpty()) currentDb = dbName;
        }

        String requestId = req.getRequestId() != null ? req.getRequestId() : UUID.randomUUID().toString();
        return router.route(requestId, currentDb, sql);
    }
}
