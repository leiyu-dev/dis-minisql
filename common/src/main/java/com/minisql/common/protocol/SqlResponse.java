package com.minisql.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Response returned by data node to coordinator, or coordinator to client.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SqlResponse {

    public enum Status { SUCCESS, ERROR }

    private String requestId;
    private Status status;
    private String message;
    /** Raw minisql output (passed through for simple display). */
    private String rawOutput;
    /** Column names, populated when rows are returned. */
    private List<String> columns;
    /** Data rows as string lists. */
    private List<List<String>> rows;
    /** Number of rows affected / returned. */
    private int affectedRows;

    public SqlResponse() {}

    public static SqlResponse success(String requestId, String rawOutput,
                                      List<String> columns, List<List<String>> rows, int affectedRows) {
        SqlResponse r = new SqlResponse();
        r.requestId = requestId;
        r.status = Status.SUCCESS;
        r.rawOutput = rawOutput;
        r.columns = columns;
        r.rows = rows;
        r.affectedRows = affectedRows;
        return r;
    }

    public static SqlResponse error(String requestId, String message) {
        SqlResponse r = new SqlResponse();
        r.requestId = requestId;
        r.status = Status.ERROR;
        r.message = message;
        return r;
    }

    public static SqlResponse pong(String requestId) {
        SqlResponse r = new SqlResponse();
        r.requestId = requestId;
        r.status = Status.SUCCESS;
        r.message = "PONG";
        return r;
    }

    public boolean isSuccess() { return status == Status.SUCCESS; }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getRawOutput() { return rawOutput; }
    public void setRawOutput(String rawOutput) { this.rawOutput = rawOutput; }

    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }

    public List<List<String>> getRows() { return rows; }
    public void setRows(List<List<String>> rows) { this.rows = rows; }

    public int getAffectedRows() { return affectedRows; }
    public void setAffectedRows(int affectedRows) { this.affectedRows = affectedRows; }
}
