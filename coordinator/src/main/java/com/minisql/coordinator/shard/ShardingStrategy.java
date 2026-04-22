package com.minisql.coordinator.shard;

/**
 * Stateless utility: maps a shard-key value to a shard ID using
 * non-negative modular hashing.
 */
public final class ShardingStrategy {

    private ShardingStrategy() {}

    /**
     * Returns the shard ID for the given key value and total shard count.
     * Uses Java's {@code hashCode()} on the string representation of the key
     * so that numeric and string keys both work.
     */
    public static int getShardId(String keyValue, int numShards) {
        if (numShards <= 0) return 0;
        return Math.abs(keyValue.hashCode()) % numShards;
    }

    /**
     * Overload for long / integer primary keys.
     */
    public static int getShardId(long keyValue, int numShards) {
        if (numShards <= 0) return 0;
        return (int) (Math.abs(keyValue) % numShards);
    }
}
