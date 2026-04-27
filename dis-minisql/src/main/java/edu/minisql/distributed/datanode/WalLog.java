package edu.minisql.distributed.datanode;

import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.common.SqlUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class WalLog {
    private final Path walFile;
    private long lastSequence;

    public WalLog(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
            this.walFile = dataDir.resolve("wal.jsonl");
            if (!Files.exists(walFile)) {
                Files.createFile(walFile);
            }
            this.lastSequence = readAll().stream().mapToLong(entry -> entry.sequence).max().orElse(0L);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize WAL", e);
        }
    }

    public synchronized WalEntry append(String requestId, int shardId, String sql) {
        Optional<WalEntry> existing = findByRequestId(requestId);
        if (existing.isPresent()) {
            return existing.get();
        }
        return appendRaw(new WalEntry(lastSequence + 1, requestId, shardId, SqlUtils.normalize(sql), System.currentTimeMillis()));
    }

    public synchronized WalEntry appendRecovered(WalEntry peerEntry) {
        Optional<WalEntry> existing = findByRequestId(peerEntry.requestId);
        if (existing.isPresent()) {
            return existing.get();
        }
        WalEntry localEntry = new WalEntry(
                lastSequence + 1,
                peerEntry.requestId,
                peerEntry.shardId,
                SqlUtils.normalize(peerEntry.sql),
                peerEntry.timestampMillis);
        return appendRaw(localEntry);
    }

    public synchronized boolean containsRequestId(String requestId) {
        return findByRequestId(requestId).isPresent();
    }

    private WalEntry appendRaw(WalEntry entry) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                walFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            writer.write(Jsons.stringify(entry));
            writer.newLine();
            writer.flush();
            lastSequence = entry.sequence;
            return entry;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append WAL", e);
        }
    }

    private Optional<WalEntry> findByRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return Optional.empty();
        }
        return readAll().stream()
                .filter(entry -> requestId.equals(entry.requestId))
                .findFirst();
    }

    public synchronized List<WalEntry> readAll() {
        try {
            List<WalEntry> entries = new ArrayList<>();
            for (String line : Files.readAllLines(walFile, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    entries.add(Jsons.parse(line, WalEntry.class));
                }
            }
            entries.sort(java.util.Comparator.comparingLong(entry -> entry.sequence));
            return entries;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read WAL", e);
        }
    }

    public synchronized long lastSequence() {
        return lastSequence;
    }
}
