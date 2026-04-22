package com.minisql.datanode.executor;

import com.minisql.common.protocol.SqlResponse;
import com.minisql.common.util.MinisqlOutputParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-level wrapper around {@link MinisqlProcess}.
 *
 * Tracks the current database so it can issue "use dbName;" automatically
 * before each statement when the database context needs to change.
 */
public class SqlExecutor {

    private static final Logger log = LoggerFactory.getLogger(SqlExecutor.class);

    private final MinisqlProcess minisql;
    private String currentDb = null;

    public SqlExecutor(MinisqlProcess minisql) {
        this.minisql = minisql;
    }

    /**
     * Execute a SQL statement in the given database context.
     * Returns a {@link SqlResponse} with parsed output.
     */
    public SqlResponse execute(String requestId, String database, String sql) {
        try {
            // Switch database if necessary
            if (database != null && !database.isEmpty() && !database.equals(currentDb)) {
                String useOutput = minisql.execute("use " + database);
                MinisqlOutputParser.ParsedResult useResult = MinisqlOutputParser.parse(useOutput);
                if (!useResult.isSuccess()) {
                    // Database may not exist yet; try to create it first then retry
                    minisql.execute("create database " + database);
                    String retryOutput = minisql.execute("use " + database);
                    MinisqlOutputParser.ParsedResult retry = MinisqlOutputParser.parse(retryOutput);
                    if (!retry.isSuccess()) {
                        return SqlResponse.error(requestId,
                                "Cannot switch to database '" + database + "': " + retry.getErrorMessage());
                    }
                }
                currentDb = database;
            }

            String rawOutput = minisql.execute(sql);
            MinisqlOutputParser.ParsedResult parsed = MinisqlOutputParser.parse(rawOutput);

            if (parsed.isSuccess()) {
                return SqlResponse.success(requestId, rawOutput,
                        parsed.getColumns(), parsed.getRows(), parsed.getAffectedRows());
            } else {
                return SqlResponse.error(requestId, parsed.getErrorMessage());
            }

        } catch (Exception e) {
            log.error("Error executing SQL: {}", sql, e);
            return SqlResponse.error(requestId, e.getMessage());
        }
    }
}
