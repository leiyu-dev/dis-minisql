package edu.minisql.distributed.coordinator;

import edu.minisql.distributed.protocol.NodeInfo;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ReplicaChooser {
    private final Map<Integer, AtomicInteger> counters = new ConcurrentHashMap<>();

    public NodeInfo chooseForRead(int shardId, List<String> replicaIds, List<NodeInfo> liveNodes) {
        List<NodeInfo> candidates = candidates(replicaIds, liveNodes);
        if (candidates.isEmpty()) {
            throw new IllegalStateException("No live replica for shard " + shardId);
        }
        int index = Math.floorMod(counters.computeIfAbsent(shardId, ignored -> new AtomicInteger()).getAndIncrement(), candidates.size());
        return candidates.get(index);
    }

    public List<NodeInfo> chooseForWrite(List<String> replicaIds, List<NodeInfo> liveNodes) {
        return candidates(replicaIds, liveNodes);
    }

    private List<NodeInfo> candidates(List<String> replicaIds, List<NodeInfo> liveNodes) {
        Map<String, NodeInfo> byId = liveNodes.stream().collect(Collectors.toMap(node -> node.nodeId, node -> node));
        return replicaIds.stream()
                .filter(byId::containsKey)
                .map(byId::get)
                .sorted(Comparator.comparing(node -> node.nodeId))
                .collect(Collectors.toList());
    }
}
