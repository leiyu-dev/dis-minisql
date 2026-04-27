package edu.minisql.distributed.protocol;

public class SqlRequest {
    public String sql;
    public String shardKey;

    public SqlRequest() {
    }

    public SqlRequest(String sql, String shardKey) {
        this.sql = sql;
        this.shardKey = shardKey;
    }
}
