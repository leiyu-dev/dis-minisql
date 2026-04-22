package com.minisql.client;

import com.minisql.common.protocol.SqlResponse;

import java.util.Scanner;

/**
 * Interactive command-line client for Distributed MiniSQL.
 *
 * Usage:
 *   java -jar client.jar --host=localhost --port=8080
 *
 * Multi-line SQL: statements are accumulated until a ';' is found.
 * Special commands:
 *   \q  or  quit;  – exit
 *   \status        – show connection info
 */
public class ClientMain {

    private static final String PROMPT       = "dis-minisql> ";
    private static final String CONT_PROMPT  = "          -> ";

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int    port = 8080;

        for (String arg : args) {
            if (arg.startsWith("--host=")) host = arg.substring(7);
            else if (arg.startsWith("--port=")) port = Integer.parseInt(arg.substring(7));
        }

        System.out.println("Distributed MiniSQL Client");
        System.out.println("Connecting to " + host + ":" + port + " ...");

        try (MiniSQLClient client = new MiniSQLClient(host, port)) {
            if (!client.ping()) {
                System.err.println("ERROR: Cannot reach coordinator at " + host + ":" + port);
                System.exit(1);
            }
            System.out.println("Connected. Type SQL statements ending with ';'. Type \\q to exit.");
            System.out.println();

            Scanner scanner = new Scanner(System.in);
            StringBuilder buf = new StringBuilder();

            while (true) {
                System.out.print(buf.length() == 0 ? PROMPT : CONT_PROMPT);
                System.out.flush();

                if (!scanner.hasNextLine()) break;
                String line = scanner.nextLine().trim();

                // Special commands
                if (line.equals("\\q") || line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                    break;
                }
                if (line.equals("\\status")) {
                    System.out.println("Connected to " + host + ":" + port);
                    System.out.println("Current database: " + (client.getCurrentDb().isEmpty() ? "(none)" : client.getCurrentDb()));
                    continue;
                }
                if (line.isEmpty()) continue;

                buf.append(line).append(" ");

                // Execute when we see ';'
                if (line.endsWith(";")) {
                    String sql = buf.toString().trim();
                    buf.setLength(0);

                    long start = System.currentTimeMillis();
                    try {
                        SqlResponse resp = client.execute(sql);
                        long elapsed = System.currentTimeMillis() - start;
                        System.out.println(MiniSQLClient.format(resp));
                        System.out.printf("(%.3f sec)%n%n", elapsed / 1000.0);
                    } catch (Exception e) {
                        System.err.println("ERROR: " + e.getMessage());
                        buf.setLength(0);
                    }
                }
            }
        }

        System.out.println("Bye.");
    }
}
