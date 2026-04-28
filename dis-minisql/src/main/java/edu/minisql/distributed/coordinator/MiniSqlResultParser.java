package edu.minisql.distributed.coordinator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MiniSqlResultParser {
    public QueryTable parse(String output) {
        QueryTable table = new QueryTable();
        if (output == null || output.isBlank()) {
            return table;
        }
        List<List<String>> parsedRows = Arrays.stream(output.split("\\R"))
                .map(String::trim)
                .filter(line -> line.startsWith("|") && line.endsWith("|"))
                .map(this::parseRow)
                .filter(row -> !row.isEmpty())
                .collect(Collectors.toList());
        if (parsedRows.isEmpty()) {
            return table;
        }
        table.columns.addAll(parsedRows.get(0));
        for (int i = 1; i < parsedRows.size(); i++) {
            if (parsedRows.get(i).size() == table.columns.size()) {
                table.rows.add(parsedRows.get(i));
            }
        }
        return table;
    }

    public QueryTable merge(List<QueryTable> tables) {
        QueryTable merged = new QueryTable();
        Set<String> seenRows = new LinkedHashSet<>();
        for (QueryTable table : tables) {
            if (table.isEmpty()) {
                continue;
            }
            if (merged.columns.isEmpty()) {
                merged.columns.addAll(table.columns);
            }
            if (sameColumns(merged.columns, table.columns)) {
                for (List<String> row : table.rows) {
                    String key = String.join("\u001f", row);
                    if (seenRows.add(key)) {
                        merged.rows.add(row);
                    }
                }
            }
        }
        return merged;
    }

    private List<String> parseRow(String line) {
        String body = line.substring(1, line.length() - 1);
        List<String> cells = new ArrayList<>();
        for (String cell : body.split("\\|")) {
            cells.add(cell.trim());
        }
        return cells;
    }

    private boolean sameColumns(List<String> left, List<String> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!left.get(i).equalsIgnoreCase(right.get(i))) {
                return false;
            }
        }
        return true;
    }
}
