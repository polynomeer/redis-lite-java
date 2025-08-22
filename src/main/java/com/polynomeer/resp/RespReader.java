package com.polynomeer.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal RESP reader for command frames:
 *   ARRAY of BULK STRINGS  (e.g., *2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n)
 * - Safe for pipelining: returns null if not enough bytes for a full frame
 * - On success advances buffer position to end of frame
 */
public class RespReader {

    /**
     * Try to parse one command frame: Array of Bulk Strings.
     * @param buf ByteBuffer in READ mode; position will advance on success.
     * @return argv list if a full frame is available, or null if need more bytes.
     */
    public List<String> tryReadCommand(ByteBuffer buf) {
        int startPos = buf.position();
        if (!buf.hasRemaining()) return null;

        // Expect array header "*<count>\r\n"
        if (peek(buf) != '*') {
            // Not a command frame
            if (!hasFullLineFrom(buf, buf.position())) return null;
            throw new RespError("Protocol error: expected Array '*'");
        }
        // Consume '*' and read integer count
        buf.get(); // '*'
        Integer count = readIntLine(buf);
        if (count == null) { buf.position(startPos); return null; }
        if (count < 0) { buf.position(startPos); return null; }

        List<String> argv = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String s = readBulkString(buf);
            if (s == null) { buf.position(startPos); return null; }
            argv.add(s);
        }
        return argv;
    }

    /** Read "$<len>\r\n<bytes>\r\n" → returns String or null if incomplete. */
    private String readBulkString(ByteBuffer buf) {
        if (!buf.hasRemaining()) return null;
        if (peek(buf) != '$') {
            // We only accept bulk strings for command args
            if (!hasFullLineFrom(buf, buf.position())) return null;
            throw new RespError("Protocol error: expected Bulk String '$'");
        }
        buf.get(); // '$'
        Integer len = readIntLine(buf);
        if (len == null) return null;
        if (len == -1) return null; // Null bulk not used for commands
        if (len < 0) throw new RespError("Negative bulk length");
        if (buf.remaining() < len + 2) return null;

        byte[] data = new byte[len];
        buf.get(data);
        expectCRLF(buf);
        return new String(data, StandardCharsets.UTF_8);
    }

    /** Read CRLF-terminated decimal int; returns null if incomplete. */
    private Integer readIntLine(ByteBuffer buf) {
        String s = readLine(buf);
        if (s == null) return null;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new RespError("Invalid integer: " + s);
        }
    }

    /** Read a line (without CRLF). Returns null if CRLF not fully available. */
    private String readLine(ByteBuffer buf) {
        int p = buf.position();
        int lim = buf.limit();
        for (int i = p; i + 1 < lim; i++) {
            if (buf.get(i) == '\r' && buf.get(i + 1) == '\n') {
                int len = i - p;
                byte[] out = new byte[len];
                buf.get(out); // advances to i
                buf.position(buf.position() + 2); // skip CRLF
                return new String(out, StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private boolean hasFullLineFrom(ByteBuffer buf, int from) {
        for (int i = from; i + 1 < buf.limit(); i++) {
            if (buf.get(i) == '\r' && buf.get(i + 1) == '\n') return true;
        }
        return false;
    }

    private byte peek(ByteBuffer buf) {
        return buf.get(buf.position());
    }

    private void expectCRLF(ByteBuffer buf) {
        if (buf.remaining() < 2) throw new RespError("Expected CRLF");
        byte c1 = buf.get(), c2 = buf.get();
        if (c1 != '\r' || c2 != '\n') throw new RespError("Expected CRLF");
    }
}
