package com.minisql.datanode.handler;

import com.minisql.common.net.MessageCodec;
import com.minisql.common.protocol.MessageType;
import com.minisql.common.protocol.SqlRequest;
import com.minisql.common.protocol.SqlResponse;
import com.minisql.datanode.executor.SqlExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

/**
 * Handles one inbound connection (coordinator → data node, or primary → replica).
 * Each connection is served by a dedicated thread.
 */
public class RequestHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    private final Socket socket;
    private final SqlExecutor executor;

    public RequestHandler(Socket socket, SqlExecutor executor) {
        this.socket = socket;
        this.executor = executor;
    }

    @Override
    public void run() {
        String remote = socket.getRemoteSocketAddress().toString();
        try (MessageCodec codec = new MessageCodec(socket)) {
            while (!socket.isClosed()) {
                SqlRequest req = codec.readRequest();
                SqlResponse resp = handleRequest(req);
                codec.writeResponse(resp);
            }
        } catch (java.io.EOFException e) {
            // Client closed the connection normally
        } catch (Exception e) {
            log.error("Error handling request from {}: {}", remote, e.getMessage());
        }
    }

    private SqlResponse handleRequest(SqlRequest req) {
        if (req.getType() == MessageType.PING) {
            return SqlResponse.pong(req.getRequestId());
        }

        // Both EXECUTE and REPLICATE go through the same local executor
        return executor.execute(req.getRequestId(), req.getDatabase(), req.getSql());
    }
}
