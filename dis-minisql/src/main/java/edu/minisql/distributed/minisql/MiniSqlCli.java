package edu.minisql.distributed.minisql;

import edu.minisql.distributed.common.SqlUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
            Process process = new ProcessBuilder(binary.toString())
                    .directory(workDir.toFile())
                    .redirectErrorStream(true)
                    .start();
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8))) {
                for (String replay : replaySql) {
                    writer.write(SqlUtils.normalize(replay));
                    writer.newLine();
                }
                if (sql != null && !sql.isBlank()) {
                    writer.write(SqlUtils.normalize(sql));
                    writer.newLine();
                }
                writer.write("quit;");
                writer.newLine();
            }

            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroyForcibly();
                throw new IllegalStateException("MiniSQL execution timed out after " + timeout);
            }
            String output = readAll(process.getInputStream());
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
