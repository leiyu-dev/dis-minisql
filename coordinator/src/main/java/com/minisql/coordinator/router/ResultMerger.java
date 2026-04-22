package com.minisql.coordinator.router;

import com.minisql.common.protocol.SqlResponse;
import com.minisql.common.util.MinisqlOutputParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Merges partial {@link SqlResponse} objects from multiple data nodes into a
 * single unified response.
 *
 * Handles:
 *   - Row-set merging (SELECT fan-out)
 *   - Affected-row aggregation (UPDATE / DELETE fan-out)
 *   - In-memory hash JOIN between two result sets
 */
public class ResultMerger {

    private static final Logger log = LoggerFactory.getLogger(ResultMerger.class);

    // ------------------------------------------------------------------
    // Row-set merge
    // ------------------------------------------------------------------

    /**
     * Combines rows from all partial responses into one response.
     * Columns are taken from the first successful response.
     */
    public SqlResponse mergeRows(String requestId, List<SqlResponse> partials) {
        List<String> columns = null;
        List<List<String>> allRows = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        int totalAffected = 0;

        for (SqlResponse resp : partials) {
            if (!resp.isSuccess()) {
                errors.add(resp.getMessage());
                continue;
            }
            if (columns == null && resp.getColumns() != null && !resp.getColumns().isEmpty()) {
                columns = resp.getColumns();
            }
            if (resp.getRows() != null) {
                allRows.addAll(resp.getRows());
            }
            totalAffected += resp.getAffectedRows();
        }

        if (!errors.isEmpty() && allRows.isEmpty()) {
            return SqlResponse.error(requestId, String.join("; ", errors));
        }

        String raw = buildRawOutput(columns, allRows);
        return SqlResponse.success(requestId, raw, columns, allRows, allRows.isEmpty() ? totalAffected : allRows.size());
    }

    /**
     * Combines affected-row counts from all partials (for UPDATE / DELETE).
     */
    public SqlResponse mergeAffected(String requestId, List<SqlResponse> partials) {
        int total = 0;
        List<String> errors = new ArrayList<>();
        for (SqlResponse resp : partials) {
            if (!resp.isSuccess()) {
                errors.add(resp.getMessage());
            } else {
                total += resp.getAffectedRows();
            }
        }
        if (!errors.isEmpty()) {
            return SqlResponse.error(requestId, String.join("; ", errors));
        }
        String raw = "Query OK, " + total + " row affected";
        return SqlResponse.success(requestId, raw, null, null, total);
    }

    // ------------------------------------------------------------------
    // JOIN support
    // ------------------------------------------------------------------

    /**
     * Performs an in-memory hash join between two result sets on the specified
     * join columns.
     *
     * @param leftCols     column list for the left table
     * @param leftRows     row data for the left table
     * @param rightCols    column list for the right table
     * @param rightRows    row data for the right table
     * @param leftJoinCol  column name in left table to join on
     * @param rightJoinCol column name in right table to join on
     * @return combined rows with columns from both tables
     */
    public JoinResult hashJoin(
            List<String> leftCols,  List<List<String>> leftRows,
            List<String> rightCols, List<List<String>> rightRows,
            String leftJoinCol, String rightJoinCol) {

        int leftIdx  = indexOfColumn(leftCols, leftJoinCol);
        int rightIdx = indexOfColumn(rightCols, rightJoinCol);

        if (leftIdx < 0 || rightIdx < 0) {
            log.warn("Join column not found. leftCol={} idx={}, rightCol={} idx={}",
                    leftJoinCol, leftIdx, rightJoinCol, rightIdx);
            return new JoinResult(Collections.emptyList(), Collections.emptyList());
        }

        // Build hash map on the smaller (right) side
        Map<String, List<List<String>>> rightMap = new HashMap<>();
        for (List<String> row : rightRows) {
            if (rightIdx < row.size()) {
                rightMap.computeIfAbsent(row.get(rightIdx), k -> new ArrayList<>()).add(row);
            }
        }

        // Probe with left rows
        List<List<String>> joined = new ArrayList<>();
        for (List<String> leftRow : leftRows) {
            if (leftIdx < leftRow.size()) {
                String key = leftRow.get(leftIdx);
                List<List<String>> matches = rightMap.get(key);
                if (matches != null) {
                    for (List<String> rightRow : matches) {
                        List<String> combined = new ArrayList<>(leftRow);
                        combined.addAll(rightRow);
                        joined.add(combined);
                    }
                }
            }
        }

        // Build merged column names (prefix to avoid ambiguity)
        List<String> mergedCols = new ArrayList<>();
        for (String c : leftCols)  mergedCols.add(c);
        for (String c : rightCols) mergedCols.add(c);

        return new JoinResult(mergedCols, joined);
    }

    private int indexOfColumn(List<String> cols, String name) {
        if (cols == null) return -1;
        String lower = name.toLowerCase();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).toLowerCase().equals(lower) ||
                cols.get(i).toLowerCase().endsWith("." + lower)) {
                return i;
            }
        }
        return -1;
    }

    // ------------------------------------------------------------------
    // Display helpers
    // ------------------------------------------------------------------

    /** Reconstructs a minisql-style table-output string. */
    private String buildRawOutput(List<String> columns, List<List<String>> rows) {
        if (columns == null || columns.isEmpty()) {
            return "Query OK, " + rows.size() + " row affected";
        }
        if (rows.isEmpty()) {
            return "Empty set";
        }
        StringBuilder sb = new StringBuilder();
        // Compute column widths
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) widths[i] = columns.get(i).length();
        for (List<String> row : rows) {
            for (int i = 0; i < Math.min(row.size(), widths.length); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }
        String divider = buildDivider(widths);
        sb.append(divider).append("\n");
        sb.append(buildRow(columns, widths)).append("\n");
        sb.append(divider).append("\n");
        for (List<String> row : rows) {
            sb.append(buildRow(row, widths)).append("\n");
        }
        sb.append(divider).append("\n");
        sb.append(rows.size()).append(" row in set");
        return sb.toString();
    }

    private String buildDivider(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int w : widths) sb.append("-".repeat(w + 2)).append("+");
        return sb.toString();
    }

    private String buildRow(List<String> cells, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String val = i < cells.size() ? cells.get(i) : "";
            sb.append(" ").append(val);
            sb.append(" ".repeat(widths[i] - val.length()));
            sb.append(" |");
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // Inner types
    // ------------------------------------------------------------------

    public static class JoinResult {
        private final List<String> columns;
        private final List<List<String>> rows;

        public JoinResult(List<String> columns, List<List<String>> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        public List<String> getColumns() { return columns; }
        public List<List<String>> getRows() { return rows; }
    }
}
