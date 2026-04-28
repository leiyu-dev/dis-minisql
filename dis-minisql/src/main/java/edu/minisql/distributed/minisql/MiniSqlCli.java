package edu.minisql.distributed.minisql;

import edu.minisql.distributed.common.SqlUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MiniSqlCli {
    private final Path binary;
    private final Path workDir;
    private final Duration timeout;

    public MiniSqlCli(Path binary, Path workDir, Duration timeout) {
        this.binary = binary.toAbsolutePath().normalize();
        this.workDir = workDir.toAbsolutePath().normalize();
        this.timeout = timeout;
    }

    public synchronized String execute(List<String> replaySql, String sql) {
        try {
            Files.createDirectories(workDir);
            List<String> statements = new ArrayList<>();
            for (String replay : replaySql) {
                statements.add(SqlUtils.normalize(replay));
            }
            if (sql != null && !sql.isBlank()) {
                statements.add(SqlUtils.normalize(sql));
            }
            Path batchFile = Files.createTempFile(workDir, "minisql-batch-", ".sql");
            Files.write(batchFile, statements, StandardCharsets.UTF_8);

            Process process = new ProcessBuilder(binary.toString(), "--batch", batchFile.toString())
                    .directory(workDir.toFile())
                    .redirectErrorStream(true)
                    .start();
            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroyForcibly();
                throw new IllegalStateException("MiniSQL execution timed out after " + timeout);
            }
            String output = readAll(process.getInputStream());
            Files.deleteIfExists(batchFile);
            if (process.exitValue() != 0) {
                throw new IllegalStateException("MiniSQL exited with code " + process.exitValue() + "\n" + output);
            }
            return output;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start MiniSQL binary: " + binary, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("MiniSQL execution interrupted", e);
        }
    }

    private String readAll(InputStream in) throws IOException {
        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
}
