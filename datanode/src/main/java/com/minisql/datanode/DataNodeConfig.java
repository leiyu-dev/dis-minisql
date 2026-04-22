package com.minisql.datanode;

/**
 * Configuration for a DataNode instance, populated from command-line arguments.
 */
public class DataNodeConfig {

    private String nodeId;
    private String host;
    private int port;
    private String zkConnectString;
    private String minisqlBinary;
    private String dataDir;
    private int replicationFactor = 1;
    /** Port used to accept replication connections from other data nodes. */
    private int replicaPort;
    private int sessionTimeoutMs = 15000;
    private int connectionTimeoutMs = 10000;

    public static DataNodeConfig fromArgs(String[] args) {
        DataNodeConfig cfg = new DataNodeConfig();
        for (String arg : args) {
            if (arg.startsWith("--node-id="))       cfg.nodeId         = arg.substring(10);
            else if (arg.startsWith("--host="))      cfg.host           = arg.substring(7);
            else if (arg.startsWith("--port="))      cfg.port           = Integer.parseInt(arg.substring(7));
            else if (arg.startsWith("--replica-port=")) cfg.replicaPort = Integer.parseInt(arg.substring(15));
            else if (arg.startsWith("--zk="))        cfg.zkConnectString = arg.substring(5);
            else if (arg.startsWith("--minisql="))   cfg.minisqlBinary  = arg.substring(10);
            else if (arg.startsWith("--data-dir="))  cfg.dataDir        = arg.substring(11);
            else if (arg.startsWith("--replication-factor=")) cfg.replicationFactor = Integer.parseInt(arg.substring(21));
        }
        if (cfg.nodeId == null) cfg.nodeId = cfg.host + ":" + cfg.port;
        if (cfg.replicaPort == 0) cfg.replicaPort = cfg.port + 100;
        return cfg;
    }

    public void validate() {
        if (host == null || host.isEmpty())            throw new IllegalArgumentException("--host is required");
        if (port <= 0)                                 throw new IllegalArgumentException("--port is required");
        if (zkConnectString == null)                   throw new IllegalArgumentException("--zk is required");
        if (minisqlBinary == null)                     throw new IllegalArgumentException("--minisql is required");
        if (dataDir == null)                           throw new IllegalArgumentException("--data-dir is required");
    }

    public String getNodeId()            { return nodeId; }
    public String getHost()              { return host; }
    public int getPort()                 { return port; }
    public int getReplicaPort()          { return replicaPort; }
    public String getZkConnectString()   { return zkConnectString; }
    public String getMinisqlBinary()     { return minisqlBinary; }
    public String getDataDir()           { return dataDir; }
    public int getReplicationFactor()    { return replicationFactor; }
    public int getSessionTimeoutMs()     { return sessionTimeoutMs; }
    public int getConnectionTimeoutMs()  { return connectionTimeoutMs; }
}
