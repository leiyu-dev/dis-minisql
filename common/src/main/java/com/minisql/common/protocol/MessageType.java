package com.minisql.common.protocol;

public enum MessageType {
    EXECUTE,    // Normal SQL execution request
    REPLICATE,  // Replication request from primary to replica
    PING,       // Health-check request
    PONG        // Health-check response
}
