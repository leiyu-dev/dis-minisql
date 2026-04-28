package edu.minisql.distributed.coordinator;

import edu.minisql.distributed.common.SqlUtils;
import edu.minisql.distributed.protocol.ExecuteResponse;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryPostProcessor {
    private static final Pattern AGGREGATE = Pattern.compile(
            "^\\s*select\\s+(count\\s*\\(\\s*\\*\\s*\\)|sum\\s*\\(\\s*([\\w.]+)\\s*\\)|min\\s*\\(\\s*([\\w.]+)\\s*\\)|max\\s*\\(\\s*([\\w.]+)\\s*\\))\\s+from\\s+([\\w]+).*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern ORDER_BY = Pattern.compile(
            "^(.*?\\s+from\\s+.+?)\\s+order\\s+by\\s+([\\w.]+)(?:\\s+(asc|desc))?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SIMPLE_JOIN = Pattern.compile(
            "^\\s*select\\s+\\*\\s+from\\s+(\\w+)\\s+join\\s+(\\w+)\\s+on\\s+([\\w.]+)\\s*=\\s*([\\w.]+)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final MiniSqlResultParser parser = new MiniSqlResultParser();

    public boolean isAggregate(String sql) {
        return AGGREGATE.matcher(sql).matches();
    }

    public boolean isSimpleJoin(String sql) {
        return SIMPLE_JOIN.matcher(sql).matches();
    }

    public String baseSqlForAggregate(String sql) {
        Matcher matcher = AGGREGATE.matcher(sql);
        if (!matcher.matches()) {
            return sql;
        }
        return "select * from " + matcher.group(5) + ";";
    }

    public String sqlWithoutOrderBy(String sql) {
        Matcher matcher = ORDER_BY.matcher(sql);
        if (!matcher.matches()) {
            return sql;
        }
        return SqlUtils.normalize(matcher.group(1));
    }

    public String leftJoinSql(String sql) {
        Matcher matcher = SIMPLE_JOIN.matcher(sql);
        if (!matcher.matches()) {
            return sql;
        }
        return "select * from " + matcher.group(1) + ";";
    }

    public String rightJoinSql(String sql) {
        Matcher matcher = SIMPLE_JOIN.matcher(sql);
        if (!matcher.matches()) {
            return sql;
        }
        return "select * from " + matcher.group(2) + ";";
    }

    public String mergeSelect(String originalSql, List<ExecuteResponse> responses) {
        QueryTable table = mergeResponses(responses);
        if (table.isEmpty()) {
            return "";
        }
        if (isAggregate(originalSql)) {
            return aggregate(originalSql, table).toAsciiTable();
        }
        applyOrderBy(originalSql, table);
        return table.toAsciiTable();
    }

    public String join(String sql, List<ExecuteResponse> leftResponses, List<ExecuteResponse> rightResponses) {
        Matcher matcher = SIMPLE_JOIN.matcher(sql);
        if (!matcher.matches()) {
            return "";
        }
        QueryTable left = mergeResponses(leftResponses);
        QueryTable right = mergeResponses(rightResponses);
        QueryTable joined = equiJoin(left, right, matcher.group(3), matcher.group(4),
                matcher.group(1), matcher.group(2));
        return joined.toAsciiTable();
    }

    private QueryTable mergeResponses(List<ExecuteResponse> responses) {
        List<QueryTable> tables = responses.stream()
                .filter(response -> response.ok)
                .map(response -> parser.parse(response.output))
                .collect(Collectors.toList());
        return parser.merge(tables);
    }

    private QueryTable aggregate(String sql, QueryTable input) {
        Matcher matcher = AGGREGATE.matcher(sql);
        QueryTable result = new QueryTable();
        if (!matcher.matches()) {
            return input;
        }
        String expression = matcher.group(1).replaceAll("\\s+", "").toLowerCase(Locale.ROOT);
        result.columns.add(expression);
        if (expression.startsWith("count(")) {
            result.rows.add(List.of(Long.toString(input.rows.size())));
            return result;
        }
        String column = Optional.ofNullable(matcher.group(2))
                .orElse(Optional.ofNullable(matcher.group(3)).orElse(matcher.group(4)));
        int columnIndex = columnIndex(input, column);
        if (columnIndex < 0) {
            result.rows.add(List.of("NULL"));
            return result;
        }
        List<Double> values = input.rows.stream()
                .map(row -> row.get(columnIndex))
                .map(this::parseDouble)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        if (values.isEmpty()) {
            result.rows.add(List.of("NULL"));
        } else if (expression.startsWith("sum(")) {
            result.rows.add(List.of(trimDouble(values.stream().mapToDouble(Double::doubleValue).sum())));
        } else if (expression.startsWith("min(")) {
            result.rows.add(List.of(trimDouble(values.stream().mapToDouble(Double::doubleValue).min().orElse(0))));
        } else if (expression.startsWith("max(")) {
            result.rows.add(List.of(trimDouble(values.stream().mapToDouble(Double::doubleValue).max().orElse(0))));
        }
        return result;
    }

    private void applyOrderBy(String sql, QueryTable table) {
        Matcher matcher = ORDER_BY.matcher(sql);
        if (!matcher.matches()) {
            return;
        }
        int columnIndex = columnIndex(table, matcher.group(2));
        if (columnIndex < 0) {
            return;
        }
        Comparator<List<String>> comparator = Comparator.comparing(row -> comparableValue(row.get(columnIndex)));
        if ("desc".equalsIgnoreCase(matcher.group(3))) {
            comparator = comparator.reversed();
        }
        table.rows.sort(comparator);
    }

    private QueryTable equiJoin(QueryTable left, QueryTable right, String leftKey, String rightKey,
                                String leftTable, String rightTable) {
        QueryTable result = new QueryTable();
        if (left.isEmpty() || right.isEmpty()) {
            return result;
        }
        int leftIndex = columnIndex(left, leftKey);
        int rightIndex = columnIndex(right, rightKey);
        if (leftIndex < 0 || rightIndex < 0) {
            return result;
        }
        left.columns.forEach(column -> result.columns.add(leftTable + "." + column));
        right.columns.forEach(column -> result.columns.add(rightTable + "." + column));

        Map<String, List<List<String>>> rightByKey = new LinkedHashMap<>();
        for (List<String> row : right.rows) {
            rightByKey.computeIfAbsent(row.get(rightIndex), ignored -> new ArrayList<>()).add(row);
        }
        for (List<String> leftRow : left.rows) {
            for (List<String> rightRow : rightByKey.getOrDefault(leftRow.get(leftIndex), List.of())) {
                List<String> joined = new ArrayList<>(leftRow);
                joined.addAll(rightRow);
                result.rows.add(joined);
            }
        }
        return result;
    }

    private int columnIndex(QueryTable table, String column) {
        String normalized = stripQualifier(column);
        for (int i = 0; i < table.columns.size(); i++) {
            if (table.columns.get(i).equalsIgnoreCase(normalized)
                    || table.columns.get(i).equalsIgnoreCase(column)) {
                return i;
            }
        }
        return -1;
    }

    private String stripQualifier(String column) {
        int dot = column.lastIndexOf('.');
        return dot < 0 ? column : column.substring(dot + 1);
    }

    private Optional<Double> parseDouble(String value) {
        try {
            return Optional.of(Double.parseDouble(value.replace("'", "")));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private ComparableValue comparableValue(String value) {
        Optional<Double> number = parseDouble(value);
        return number.map(ComparableValue::number).orElseGet(() -> ComparableValue.text(value));
    }

    private String trimDouble(double value) {
        if (value == Math.rint(value)) {
            return Long.toString((long) value);
        }
        return Double.toString(value);
    }

    private static class ComparableValue implements Comparable<ComparableValue> {
        private final Double number;
        private final String text;

        private ComparableValue(Double number, String text) {
            this.number = number;
            this.text = text;
        }

        static ComparableValue number(Double value) {
            return new ComparableValue(value, null);
        }

        static ComparableValue text(String value) {
            return new ComparableValue(null, value);
        }

        @Override
        public int compareTo(ComparableValue other) {
            if (number != null && other.number != null) {
                return number.compareTo(other.number);
            }
            return String.valueOf(text).compareTo(String.valueOf(other.text));
        }
    }
}
