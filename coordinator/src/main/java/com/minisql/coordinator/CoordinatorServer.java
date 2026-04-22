package com.minisql.coordinator;

import com.minisql.coordinator.handler.ClientHandler;
import com.minisql.coordinator.router.SqlRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TCP server that accepts client SQL connections and dispatches each to a
 * {@link ClientHandler} thread.
 */
public class CoordinatorServer {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorServer.class);

    private final int port;
    private final SqlRouter router;
    private final ExecutorService threadPool;
    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public CoordinatorServer(int port, SqlRouter router) {
        this.port = port;
        this.router = router;
        this.threadPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "client-handler");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws Exception {
        serverSocket = new ServerSocket(port);
        running = true;
        log.info("Coordinator listening on port {}", port);

        while (running) {
            try {
                Socket client = serverSocket.accept();
                client.setTcpNoDelay(true);
                threadPool.submit(new ClientHandler(client, router));
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
