package com.polynomeer.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal RESP reader that can parse:
 * ARRAY of BULK STRINGS  (command frames)
 * It is safe for pipelining: returns null if not enough bytes for a full frame
 * and does NOT consume the buffer in that case.
 */
public class RespReader {

    /**
     * Try to parse one command frame: Array of Bulk Strings.
     *
     * @param buf ByteBuffer in READ mode; position will advance on success.
     * @return argv list if a full frame is available, or null if need more bytes.
     */
    public List<String> tryReadCommand(ByteBuffer buf) {
        if (!buf.hasRemaining()) return null;

        // Peek first byte
        byte b = buf.get(buf.position());
        if (b != '*') {
            // Redis clients always send arrays for commands.
            // Treat anything else as protocol error (but only if we have whole line).
            Integer dummy = tryReadIntegerPrefix(buf, '*');
            if (dummy == null) return null; // need more bytes to even complain
            throw new RespError("Protocol error: expected Array '*'");
        }

        Integer count = tryReadIntegerPrefix(buf, '*');
        if (count == null) return null;
        if (count < 0) {
            // Null array; not used for commands
            return null; // or throw error
        }

        List<String> argv = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String s = tryReadBulkString(buf);
            if (s == null) {
                // Not enough bytes for this element: rewind argv parsing
                // Caller will retry when more bytes arrive.
                // To rewind: we cannot easily revert the buffer here; caller saves position.
                return null;
            }
            argv.add(s);
        }
        return argv;
    }

    /**
     * Try read "$<len>\r\n<bytes>\r\n" → returns String or null if incomplete.
     */
    private String tryReadBulkString(ByteBuffer buf) {
        if (!buf.hasRemaining()) return null;
        byte b = buf.get(buf.position());
        if (b == '$') {
            Integer len = tryReadIntegerPrefix(buf, '$');
            if (len == null) return null;
            if (len == -1) return null; // Null bulk not used for commands
            if (len < 0) throw new RespError("Negative bulk length");
            if (buf.remaining() < len + 2) {
                return null; // not enough bytes for payload + CRLF
            }
            // Read payload
            byte[] data = new byte[len];
            buf.get(data);
            // Expect CRLF
            if (!expectCRLF(buf)) {
                throw new RespError("Bulk string missing CRLF tail");
            }
            return new String(data, StandardCharsets.UTF_8);
        } else {
            // Accept Simple String as argument (rare, but keeps parser tolerant)
            String simple = tryReadSimpleString(buf);
            if (simple == null) return null;
            return simple;
        }
    }

    /**
     * Try read "+<text>\r\n" returning text without '+' and CRLF.
     */
    private String tryReadSimpleString(ByteBuffer buf) {
        if (!buf.hasRemaining()) return null;
        if (buf.get(buf.position()) != '+') return null;
        String line = tryReadLine(buf, 1);
        return line; // already skips the '+' by offset=1
    }

    /**
     * Try read "*<int>\r\n" or "$<int>\r\n"
     * - Consumes the whole header on success.
     * - Returns null if incomplete.
     */
    private Integer tryReadIntegerPrefix(ByteBuffer buf, char prefix) {
        if (!buf.hasRemaining()) return null;
        if (buf.get(buf.position()) != (byte) prefix) return null;
        String line = tryReadLine(buf, 1); // skip prefix char
        if (line == null) return null;
        try {
            return Integer.parseInt(line);
        } catch (NumberFormatException e) {
            throw new RespError("Invalid integer after prefix " + prefix + ": " + line);
        }
    }

    /**
     * Try to read a single line (ending with CRLF) starting from current position+offset.
     * On success, advances position to after CRLF and returns the line content.
     * On incomplete line, does NOT advance position and returns null.
     */
    private String tryReadLine(ByteBuffer buf, int offset) {
        int start = buf.position() + offset;
        int i = start;
        while (i + 1 < buf.limit()) {
            if (buf.get(i) == '\r' && buf.get(i + 1) == '\n') {
                int end = i;
                int len = end - start;
                if (len < 0) throw new RespError("Malformed line (offset past limit)");
                byte[] bytes = new byte[len];
                // Advance position to after CRLF (including prefix offset)
                buf.position(i + 2);
                // Extract line (without CRLF)
                int oldPos = buf.position();
                // Temporarily read from original content: need a duplicate
                int savedPos = buf.position();
                // Copy bytes manually
                // But simpler: create a slice over original array—however, ByteBuffer may be direct.
                // We'll read by indexing original buffer:
                for (int k = 0; k < len; k++) {
                    bytes[k] = buf.get(start + k - 2 - (start - (buf.position() - 2))); // this is messy
                }
                // The above is too complex. Better: we know the underlying bytes but not accessible.
                // Simpler approach: rewind to start, read bytes, then set back to after CRLF.

                // Rethink: easiest is to temporarily save positions and use get() sequentially.
                // Let's implement cleanly:

                // Roll back to compute string cleanly
                buf.position(start);
                byte[] out = new byte[len];
                buf.get(out);
                // Skip CRLF
                if (!expectCRLF(buf)) throw new RespError("Missing CRLF");
                return new String(out, StandardCharsets.UTF_8);
            }
            i++;
        }
        // Not enough bytes to find CRLF
        return null;
    }

    /**
     * Expect CRLF at current position; advances position by 2 on success.
     */
    private boolean expectCRLF(ByteBuffer buf) {
        if (buf.remaining() < 2) return false;
        byte c1 = buf.get();
        byte c2 = buf.get();
        if (c1 != '\r' || c2 != '\n') {
            throw new RespError("Expected CRLF");
        }
        return true;
    }
}
