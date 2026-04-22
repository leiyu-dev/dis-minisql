package com.minisql.datanode.replica;

import com.minisql.common.net.MessageCodec;
import com.minisql.common.protocol.MessageType;
import com.minisql.common.protocol.SqlRequest;
import com.minisql.common.protocol.SqlResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.UUID;

/**
 * Sends replication SQL statements to a single remote replica data node.
 */
public class ReplicaClient {

    private static final Logger log = LoggerFactory.getLogger(ReplicaClient.class);

    private final String host;
    private final int port;

    public ReplicaClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Asynchronously replicate a SQL statement to the remote replica.
     * Failures are logged but do not propagate (best-effort replication).
     */
    public void replicateAsync(String database, String sql) {
        Thread t = new Thread(() -> replicate(database, sql), "replica-sender");
        t.setDaemon(true);
        t.start();
    }

    /** Synchronous replication; returns true on success. */
    public boolean replicate(String database, String sql) {
        try (Socket socket = new Socket(host, port);
             MessageCodec codec = new MessageCodec(socket)) {

            SqlRequest req = new SqlRequest(
                    UUID.randomUUID().toString(), MessageType.REPLICATE, sql, database);
            codec.writeRequest(req);

            SqlResponse resp = codec.readResponse();
            if (!resp.isSuccess()) {
                log.warn("Replication to {}:{} failed: {}", host, port, resp.getMessage());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.error("Replication to {}:{} error: {}", host, port, e.getMessage());
            return false;
        }
    }
}
