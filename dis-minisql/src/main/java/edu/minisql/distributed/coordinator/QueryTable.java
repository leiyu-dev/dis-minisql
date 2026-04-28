package edu.minisql.distributed.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class QueryTable {
    public List<String> columns = new ArrayList<>();
    public List<List<String>> rows = new ArrayList<>();

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public String toAsciiTable() {
        if (columns.isEmpty()) {
            return "Empty set";
        }
        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            widths[i] = columns.get(i).length();
        }
        for (List<String> row : rows) {
            for (int i = 0; i < Math.min(row.size(), widths.length); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }
        StringBuilder out = new StringBuilder();
        appendBorder(out, widths);
        appendRow(out, columns, widths);
        appendBorder(out, widths);
        for (List<String> row : rows) {
            appendRow(out, row, widths);
        }
        appendBorder(out, widths);
        out.append(rows.size()).append(rows.size() == 1 ? " row" : " rows").append(" merged by coordinator");
        return out.toString();
    }

    private void appendBorder(StringBuilder out, int[] widths) {
        for (int width : widths) {
            out.append('+').append("-".repeat(width + 2));
        }
        out.append("+\n");
    }

    private void appendRow(StringBuilder out, List<String> row, int[] widths) {
        for (int i = 0; i < widths.length; i++) {
            String value = i < row.size() ? row.get(i) : "";
            out.append("| ").append(padRight(value, widths[i])).append(' ');
        }
        out.append("|\n");
    }

    private String padRight(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        return value + " ".repeat(width - value.length());
    }

    public String toCsvLikeRows() {
        StringBuilder out = new StringBuilder();
        out.append(String.join(",", columns)).append('\n');
        for (List<String> row : rows) {
            StringJoiner joiner = new StringJoiner(",");
            row.forEach(joiner::add);
            out.append(joiner).append('\n');
        }
        return out.toString();
    }
}
