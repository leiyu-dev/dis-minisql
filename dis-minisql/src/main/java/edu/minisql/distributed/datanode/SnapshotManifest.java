package edu.minisql.distributed.datanode;

import java.util.Map;
import java.util.TreeMap;

public class SnapshotManifest {
    /** Highest local WAL sequence already reflected in the MiniSQL data directory. */
    public long lastWalSequence;
    /** Highest local WAL sequence copied into the snapshot and eligible for compaction. */
    public long lastCompactedWalSequence;
    public long createdAtMillis;
    public Map<Integer, Long> shardLogIndexes = new TreeMap<>();
}
