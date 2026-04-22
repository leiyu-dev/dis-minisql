package com.minisql.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the text output produced by the minisql C++ process into a structured result.
 *
 * minisql outputs three kinds of responses:
 *   1. Table result  – rows surrounded by '+---+' dividers and '|' separators
 *   2. Query OK      – "Query OK, N row affected(...)"
 *   3. Error / info  – plain text (including parser errors)
 */
public final class MinisqlOutputParser {

    private static final Pattern AFFECTED_PATTERN =
            Pattern.compile("(?:Query OK,\\s*(\\d+)|(?:(\\d+)) row[s]? in set)", Pattern.CASE_INSENSITIVE);

    private MinisqlOutputParser() {}

    public static ParsedResult parse(String raw) {
        if (raw == null) raw = "";
        String trimmed = raw.trim();

        ParsedResult result = new ParsedResult();
        result.setRawOutput(raw);

        if (trimmed.isEmpty()) {
            result.setSuccess(true);
            return result;
        }

        // Detect hard errors: minisql prints error messages without a '+' table
        boolean hasTable = trimmed.contains("+") && trimmed.contains("|");
        boolean hasQueryOk = trimmed.contains("Query OK");
        boolean hasRowSet = trimmed.contains("row in set") || trimmed.contains("rows in set");
        boolean hasEmptySet = trimmed.contains("Empty set");

        if (!hasTable && !hasQueryOk && !hasRowSet && !hasEmptySet) {
            // Treat as error / informational message
            result.setSuccess(false);
            result.setErrorMessage(trimmed);
            return result;
        }

        result.setSuccess(true);

        // Parse affected / returned row count
        Matcher m = AFFECTED_PATTERN.matcher(trimmed);
        if (m.find()) {
            String g = m.group(1) != null ? m.group(1) : m.group(2);
            if (g != null) result.setAffectedRows(Integer.parseInt(g));
        }

        if (!hasTable) {
            // INSERT / UPDATE / DELETE / DDL – no rows to parse
            return result;
        }

        // Parse table output
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        boolean headerParsed = false;

        for (String line : trimmed.split("\n")) {
            String l = line.trim();
            if (l.startsWith("+")) continue; // divider
            if (l.startsWith("|")) {
                List<String> cells = splitRow(l);
                if (!headerParsed) {
                    columns = cells;
                    headerParsed = true;
                } else {
                    rows.add(cells);
                }
            }
        }

        result.setColumns(columns);
        result.setRows(rows);
        result.setAffectedRows(rows.size());
        return result;
    }

    /** Split a '| a | b | c |' line into ["a","b","c"]. */
    private static List<String> splitRow(String line) {
        List<String> cells = new ArrayList<>();
        String[] parts = line.split("\\|");
        for (String p : parts) {
            String v = p.trim();
            if (!v.isEmpty()) cells.add(v);
        }
        return cells;
    }

    // ------------------------------------------------------------------
    // Result holder
    // ------------------------------------------------------------------

    public static class ParsedResult {
        private boolean success;
        private String rawOutput;
        private String errorMessage;
        private List<String> columns = new ArrayList<>();
        private List<List<String>> rows = new ArrayList<>();
        private int affectedRows;

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }

        public String getRawOutput() { return rawOutput; }
        public void setRawOutput(String rawOutput) { this.rawOutput = rawOutput; }

        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

        public List<String> getColumns() { return columns; }
        public void setColumns(List<String> columns) { this.columns = columns; }

        public List<List<String>> getRows() { return rows; }
        public void setRows(List<List<String>> rows) { this.rows = rows; }

        public int getAffectedRows() { return affectedRows; }
        public void setAffectedRows(int affectedRows) { this.affectedRows = affectedRows; }
    }
}
