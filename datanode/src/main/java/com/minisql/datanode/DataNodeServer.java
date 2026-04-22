package com.minisql.datanode;

import com.minisql.datanode.executor.MinisqlProcess;
import com.minisql.datanode.executor.SqlExecutor;
import com.minisql.datanode.handler.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TCP server that accepts connections from the coordinator (and peer data nodes
 * for replication).  Each accepted connection is handed to a {@link RequestHandler}
 * thread.
 */
public class DataNodeServer {

    private static final Logger log = LoggerFactory.getLogger(DataNodeServer.class);

    private final DataNodeConfig config;
    private final SqlExecutor executor;
    private final ExecutorService threadPool;
    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public DataNodeServer(DataNodeConfig config) throws Exception {
        this.config = config;

        // Start the minisql subprocess
        MinisqlProcess minisql = new MinisqlProcess();
        minisql.start(config.getMinisqlBinary(),
                      config.getDataDir() + "/" + config.getNodeId());

        this.executor   = new SqlExecutor(minisql);
        this.threadPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "datanode-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws Exception {
        serverSocket = new ServerSocket(config.getPort());
        running = true;
        log.info("DataNode '{}' listening on port {}", config.getNodeId(), config.getPort());

        while (running) {
            try {
                Socket client = serverSocket.accept();
                client.setTcpNoDelay(true);
                threadPool.submit(new RequestHandler(client, executor));
            } catch (Exception e) {
                if (running) log.error("Accept error", e);
            }
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (Exception ignored) {}
        threadPool.shutdownNow();
    }
}
