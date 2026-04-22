package com.minisql.coordinator;

/**
 * Configuration for the Coordinator, populated from command-line arguments.
 */
public class CoordinatorConfig {

    private String host = "0.0.0.0";
    private int port = 8080;
    private String zkConnectString;
    private int replicationFactor = 1;
    private int sessionTimeoutMs = 15000;
    private int connectionTimeoutMs = 10000;

    public static CoordinatorConfig fromArgs(String[] args) {
        CoordinatorConfig cfg = new CoordinatorConfig();
        for (String arg : args) {
            if (arg.startsWith("--host="))               cfg.host = arg.substring(7);
            else if (arg.startsWith("--port="))          cfg.port = Integer.parseInt(arg.substring(7));
            else if (arg.startsWith("--zk="))            cfg.zkConnectString = arg.substring(5);
            else if (arg.startsWith("--replication-factor="))
                cfg.replicationFactor = Integer.parseInt(arg.substring(21));
        }
        return cfg;
    }

    public void validate() {
        if (zkConnectString == null) throw new IllegalArgumentException("--zk is required");
    }

    public String getHost()             { return host; }
    public int getPort()                { return port; }
    public String getZkConnectString()  { return zkConnectString; }
    public int getReplicationFactor()   { return replicationFactor; }
    public int getSessionTimeoutMs()    { return sessionTimeoutMs; }
    public int getConnectionTimeoutMs() { return connectionTimeoutMs; }
}
