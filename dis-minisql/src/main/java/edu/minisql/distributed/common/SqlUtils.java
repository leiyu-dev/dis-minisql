package edu.minisql.distributed.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlUtils {
    private static final Pattern INSERT_VALUES = Pattern.compile(
            "^\\s*insert\\s+into\\s+([a-zA-Z_][\\w]*)\\s+values\\s*\\((.*)\\)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern CREATE_TABLE = Pattern.compile(
            "^\\s*create\\s+table\\s+([a-zA-Z_][\\w]*)\\s*\\((.*)\\)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SELECT_FROM = Pattern.compile(
            "^\\s*select\\s+.*?\\s+from\\s+(.+?)\\s*(?:where\\s+.+)?;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private SqlUtils() {
    }

    public static String normalize(String sql) {
        String trimmed = sql == null ? "" : sql.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        return trimmed.endsWith(";") ? trimmed : trimmed + ";";
    }

    public static boolean isReadOnly(String sql) {
        String op = firstKeyword(sql);
        return "select".equals(op) || "show".equals(op) || "desc".equals(op) || "describe".equals(op);
    }

    public static boolean isBroadcastDdl(String sql) {
        String op = firstKeyword(sql);
        return "create".equals(op) || "drop".equals(op);
    }

    public static String firstKeyword(String sql) {
        String trimmed = sql == null ? "" : sql.stripLeading().toLowerCase(Locale.ROOT);
        int split = trimmed.indexOf(' ');
        int end = split < 0 ? trimmed.length() : split;
        return end == 0 ? "" : trimmed.substring(0, end);
    }

    public static String inferInsertTable(String sql) {
        Matcher matcher = INSERT_VALUES.matcher(sql);
        return matcher.matches() ? matcher.group(1) : null;
    }

    public static String inferCreateTable(String sql) {
        Matcher matcher = CREATE_TABLE.matcher(sql);
        return matcher.matches() ? matcher.group(1) : null;
    }

    public static String inferShardKey(String sql) {
        Matcher matcher = INSERT_VALUES.matcher(sql);
        if (!matcher.matches()) {
            return null;
        }
        String values = matcher.group(2);
        int comma = values.indexOf(',');
        String firstValue = comma < 0 ? values : values.substring(0, comma);
        return firstValue.trim().replaceAll("^['\"]|['\"]$", "");
    }

    public static String[] inferSelectTables(String sql) {
        Matcher matcher = SELECT_FROM.matcher(sql);
        if (!matcher.matches()) {
            return new String[0];
        }
        return matcher.group(1)
                .replaceAll("(?i)\\bjoin\\b", ",")
                .split("\\s*,\\s*");
    }

    public static int hashToShard(String key, int shardCount) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
        byte[] digest = sha256(key == null ? "" : key);
        int value = ((digest[0] & 0xff) << 24)
                | ((digest[1] & 0xff) << 16)
                | ((digest[2] & 0xff) << 8)
                | (digest[3] & 0xff);
        return Math.floorMod(value, shardCount);
    }

    private static byte[] sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
