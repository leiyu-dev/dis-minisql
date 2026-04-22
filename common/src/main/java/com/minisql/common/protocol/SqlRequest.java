package com.minisql.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Request sent from coordinator to data node (or client to coordinator).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SqlRequest {

    private String requestId;
    private MessageType type;
    /** Raw SQL statement (may be empty for PING). */
    private String sql;
    /** Target database name; empty means no USE switch needed. */
    private String database;

    public SqlRequest() {}

    public SqlRequest(String requestId, MessageType type, String sql, String database) {
        this.requestId = requestId;
        this.type = type;
        this.sql = sql;
        this.database = database;
    }

    public static SqlRequest ping(String requestId) {
        return new SqlRequest(requestId, MessageType.PING, "", "");
    }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public MessageType getType() { return type; }
    public void setType(MessageType type) { this.type = type; }

    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }

    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }

    @Override
    public String toString() {
        return "SqlRequest{type=" + type + ", db=" + database + ", sql=" + sql + "}";
    }
}
