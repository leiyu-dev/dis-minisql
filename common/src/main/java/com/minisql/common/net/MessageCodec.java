package com.minisql.common.net;

import com.minisql.common.protocol.SqlRequest;
import com.minisql.common.protocol.SqlResponse;
import com.minisql.common.util.JsonUtil;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Length-prefixed message framing over a TCP socket.
 *
 * Wire format (big-endian):
 *   [4-byte int: payload length][payload bytes: UTF-8 JSON]
 */
public class MessageCodec implements Closeable {

    private final DataInputStream in;
    private final DataOutputStream out;

    public MessageCodec(Socket socket) throws IOException {
        this.in  = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    // ------------------------------------------------------------------
    // Write helpers
    // ------------------------------------------------------------------

    public void writeRequest(SqlRequest request) throws IOException {
        writeFrame(JsonUtil.toJson(request));
    }

    public void writeResponse(SqlResponse response) throws IOException {
        writeFrame(JsonUtil.toJson(response));
    }

    private void writeFrame(String json) throws IOException {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    // ------------------------------------------------------------------
    // Read helpers
    // ------------------------------------------------------------------

    public SqlRequest readRequest() throws IOException {
        return JsonUtil.fromJson(readFrame(), SqlRequest.class);
    }

    public SqlResponse readResponse() throws IOException {
        return JsonUtil.fromJson(readFrame(), SqlResponse.class);
    }

    private String readFrame() throws IOException {
        int len = in.readInt();
        if (len <= 0 || len > 64 * 1024 * 1024) {
            throw new IOException("Invalid frame length: " + len);
        }
        byte[] payload = new byte[len];
        in.readFully(payload);
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        in.close();
        out.close();
    }
}
