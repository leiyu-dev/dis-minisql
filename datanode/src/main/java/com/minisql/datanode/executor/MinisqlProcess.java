package com.minisql.datanode.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Manages a single minisql child process.
 *
 * Communication uses the native stdin/stdout protocol:
 *   1. minisql prints "minisql > " and blocks on stdin.
 *   2. We write a SQL statement ending with ';' followed by '\n'.
 *   3. minisql processes it, writes the result to stdout, then prints
 *      "minisql > " again and waits.
 *
 * We detect command completion by watching for the next "minisql > " prompt
 * that appears after we sent our command.
 */
public class MinisqlProcess {

    private static final Logger log = LoggerFactory.getLogger(MinisqlProcess.class);

    private static final String PROMPT        = "minisql > ";
    private static final long   EXEC_TIMEOUT  = 30_000L; // ms per command

    private Process        process;
    private BufferedWriter writer;

    private final StringBuilder buffer    = new StringBuilder();
    private final Object         lock     = new Object();
    private volatile boolean     running  = false;

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /**
     * Starts the minisql process in the given working directory and waits for
     * the initial prompt.
     */
    public void start(String binaryPath, String workingDir) throws Exception {
        File wd = new File(workingDir);
        if (!wd.exists()) wd.mkdirs();

        ProcessBuilder pb = new ProcessBuilder(binaryPath);
        pb.directory(wd);
        pb.redirectErrorStream(true); // merge stderr -> stdout so we catch errors too
        process = pb.start();
        running = true;

        writer = new BufferedWriter(
                new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));

        Thread reader = new Thread(this::readLoop, "minisql-reader");
        reader.setDaemon(true);
        reader.start();

        // Wait for the initial prompt
        waitForPromptFrom(0);
        log.info("minisql process started, working dir={}", workingDir);
    }

    public void stop() {
        running = false;
        if (process != null && process.isAlive()) {
            try {
                writer.write("quit;\n");
                writer.flush();
            } catch (IOException ignored) {}
            process.destroyForcibly();
        }
    }

    public boolean isAlive() {
        return process != null && process.isAlive();
    }

    // ------------------------------------------------------------------
    // SQL execution
    // ------------------------------------------------------------------

    /**
     * Sends a SQL statement to minisql and returns the raw output text
     * (everything printed before the next "minisql > " prompt).
     * This method is thread-safe (serialized on {@code this}).
     */
    public synchronized String execute(String sql) throws Exception {
        if (!isAlive()) throw new IOException("minisql process is not running");

        int startPos;
        synchronized (lock) {
            startPos = buffer.length();
        }

        String cmd = sql.trim();
        if (!cmd.endsWith(";")) cmd += ";";
        writer.write(cmd);
        writer.newLine();
        writer.flush();
        log.debug(">>> {}", cmd);

        String output = waitForPromptFrom(startPos);
        log.debug("<<< {}", output.trim());
        return output;
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /** Background thread: continuously appends minisql stdout to buffer. */
    private void readLoop() {
        try (InputStream is = process.getInputStream()) {
            byte[] buf = new byte[512];
            int len;
            while (running && (len = is.read(buf)) != -1) {
                String chunk = new String(buf, 0, len, StandardCharsets.UTF_8);
                synchronized (lock) {
                    buffer.append(chunk);
                    lock.notifyAll();
                }
            }
        } catch (IOException e) {
            if (running) log.error("minisql reader error", e);
        } finally {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    /**
     * Blocks until "minisql > " appears at or after {@code fromPos} in the
     * accumulation buffer, then returns the text between fromPos and that prompt.
     */
    private String waitForPromptFrom(int fromPos) throws InterruptedException {
        long deadline = System.currentTimeMillis() + EXEC_TIMEOUT;
        synchronized (lock) {
            while (true) {
                int promptIdx = buffer.indexOf(PROMPT, fromPos);
                if (promptIdx >= 0) {
                    return buffer.substring(fromPos, promptIdx);
                }
                if (!isAlive()) {
                    return buffer.substring(fromPos);
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new InterruptedException("Timeout waiting for minisql prompt");
                }
                lock.wait(Math.min(remaining, 1000));
            }
        }
    }
}
