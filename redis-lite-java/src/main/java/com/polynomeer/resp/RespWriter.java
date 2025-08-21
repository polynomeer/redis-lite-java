package com.polynomeer.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Minimal RESP writer helpers.
 * Returned ByteBuffers are flipped for reading (position = 0, limit = size).
 */
public class RespWriter {

    public static ByteBuffer simpleString(String s) {
        // +<s>\r\n
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + b.length + 2);
        buf.put((byte) '+').put(b).put((byte) '\r').put((byte) '\n');
        buf.flip();
        return buf;
    }

    public static ByteBuffer error(String s) {
        // -ERR <s>\r\n
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + b.length + 2);
        buf.put((byte) '-').put(b).put((byte) '\r').put((byte) '\n');
        buf.flip();
        return buf;
    }

    public static ByteBuffer bulkString(String s) {
        if (s == null) return nullBulk();
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        byte[] len = Integer.toString(b.length).getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + len.length + 2 + b.length + 2);
        buf.put((byte) '$').put(len).put((byte) '\r').put((byte) '\n');
        buf.put(b).put((byte) '\r').put((byte) '\n');
        buf.flip();
        return buf;
    }

    public static ByteBuffer integer(long v) {
        byte[] b = Long.toString(v).getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + b.length + 2);
        buf.put((byte) ':').put(b).put((byte) '\r').put((byte) '\n');
        buf.flip();
        return buf;
    }

    public static ByteBuffer nullBulk() {
        ByteBuffer buf = ByteBuffer.allocate(5);
        buf.put((byte) '$').put((byte) '-').put((byte) '1').put((byte) '\r').put((byte) '\n');
        buf.flip();
        return buf;
    }
}
