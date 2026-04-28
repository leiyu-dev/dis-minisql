package edu.minisql.distributed.datanode;

import edu.minisql.distributed.common.Jsons;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Map;

public class SnapshotManager {
    private final Path dataDir;
    private final Path manifestFile;
    private final Path snapshotDir;

    public SnapshotManager(Path dataDir) {
        this.dataDir = dataDir.toAbsolutePath().normalize();
        this.manifestFile = this.dataDir.resolve("snapshot.json");
        this.snapshotDir = this.dataDir.resolve("snapshots").resolve("current");
    }

    public SnapshotManifest loadManifest() {
        try {
            Files.createDirectories(dataDir);
            if (!Files.exists(manifestFile)) {
                SnapshotManifest manifest = new SnapshotManifest();
                saveManifest(manifest);
                return manifest;
            }
            return Jsons.parse(Files.readString(manifestFile), SnapshotManifest.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load snapshot manifest", e);
        }
    }

    public void restoreIfNeeded() {
        Path databases = dataDir.resolve("databases");
        Path snapshotDatabases = snapshotDir.resolve("databases");
        if (Files.exists(databases) || !Files.exists(snapshotDatabases)) {
            return;
        }
        copyDirectory(snapshotDatabases, databases);
    }

    public SnapshotManifest createSnapshot(long lastWalSequence, Map<Integer, Long> shardLogIndexes) {
        try {
            Files.createDirectories(snapshotDir);
            Path databases = dataDir.resolve("databases");
            if (Files.exists(databases)) {
                Path snapshotDatabases = snapshotDir.resolve("databases");
                deleteDirectory(snapshotDatabases);
                copyDirectory(databases, snapshotDatabases);
            }
            SnapshotManifest manifest = new SnapshotManifest();
            manifest.lastWalSequence = lastWalSequence;
            manifest.lastCompactedWalSequence = lastWalSequence;
            manifest.createdAtMillis = System.currentTimeMillis();
            manifest.shardLogIndexes.putAll(shardLogIndexes);
            saveManifest(manifest);
            return manifest;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create snapshot", e);
        }
    }

    public SnapshotManifest markApplied(SnapshotManifest manifest, long lastWalSequence,
                                         Map<Integer, Long> shardLogIndexes) {
        try {
            manifest.lastWalSequence = Math.max(manifest.lastWalSequence, lastWalSequence);
            manifest.createdAtMillis = System.currentTimeMillis();
            manifest.shardLogIndexes.putAll(shardLogIndexes);
            saveManifest(manifest);
            return manifest;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to update snapshot manifest", e);
        }
    }

    private void saveManifest(SnapshotManifest manifest) throws IOException {
        Files.createDirectories(dataDir);
        Files.writeString(manifestFile, Jsons.stringify(manifest));
    }

    private void copyDirectory(Path from, Path to) {
        try {
            Files.createDirectories(to);
            try (var paths = Files.walk(from)) {
                for (Path source : paths.collect(java.util.stream.Collectors.toList())) {
                    Path target = to.resolve(from.relativize(source));
                    if (Files.isDirectory(source)) {
                        Files.createDirectories(target);
                    } else {
                        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to copy snapshot directory", e);
        }
    }

    private void deleteDirectory(Path path) {
        if (!Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).collect(java.util.stream.Collectors.toList())) {
                Files.deleteIfExists(p);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to delete snapshot directory", e);
        }
    }
}
