package com.polynomeer.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Minimal RESP writer helpers.
 * Returned ByteBuffers are flipped for reading (position = 0, limit = size).
 */
public class RespWriter {

    public static ByteBuffer simpleString(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + b.length + 2);
        buf.put((byte) '+').put(b).put((byte) '\r').put((byte) '\n');
        buf.flip();
        return buf;
    }

    public static ByteBuffer error(String s) {
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

    // ---------- Arrays / mixed elements (bulk strings + integer) ----------

    /**
     * RESP Array of 3 elements: bulk, bulk, integer (used for subscribe/unsubscribe ack).
     */
    public static ByteBuffer array3(String a, String b, long c) {
        byte[] A = a.getBytes(StandardCharsets.UTF_8);
        byte[] B = b.getBytes(StandardCharsets.UTF_8);
        byte[] Ci = Long.toString(c).getBytes(StandardCharsets.UTF_8);

        byte[] lenA = Integer.toString(A.length).getBytes(StandardCharsets.UTF_8);
        byte[] lenB = Integer.toString(B.length).getBytes(StandardCharsets.UTF_8);

        // "*3\r\n" is 4 bytes (NOT 3)
        int cap = 4
                + (1 + lenA.length + 2) + (A.length + 2)   // $lenA\r\n + A\r\n
                + (1 + lenB.length + 2) + (B.length + 2)   // $lenB\r\n + B\r\n
                + (1 + Ci.length + 2);                     // :Ci\r\n

        ByteBuffer buf = ByteBuffer.allocate(cap);

        buf.put((byte) '*').put((byte) '3').put((byte) '\r').put((byte) '\n');

        buf.put((byte) '$').put(lenA).put((byte) '\r').put((byte) '\n');
        buf.put(A).put((byte) '\r').put((byte) '\n');

        buf.put((byte) '$').put(lenB).put((byte) '\r').put((byte) '\n');
        buf.put(B).put((byte) '\r').put((byte) '\n');

        buf.put((byte) ':').put(Ci).put((byte) '\r').put((byte) '\n');

        buf.flip();
        return buf;
    }

    /**
     * RESP Array ["message", channel, payload] for PUBLISH delivery.
     */
    public static ByteBuffer arrayMessage(String channel, String payload) {
        return arrayBulk3("message", channel, payload);
    }

    /**
     * RESP Array of 3 bulk strings. (used for ["message", channel, payload])
     */
    public static ByteBuffer arrayBulk3(String a, String b, String c) {
        byte[] A = a.getBytes(StandardCharsets.UTF_8);
        byte[] B = b.getBytes(StandardCharsets.UTF_8);
        byte[] C = c.getBytes(StandardCharsets.UTF_8);

        byte[] lenA = Integer.toString(A.length).getBytes(StandardCharsets.UTF_8);
        byte[] lenB = Integer.toString(B.length).getBytes(StandardCharsets.UTF_8);
        byte[] lenC = Integer.toString(C.length).getBytes(StandardCharsets.UTF_8);

        // "*3\r\n" is 4 bytes
        int cap = 4
                + (1 + lenA.length + 2) + (A.length + 2)
                + (1 + lenB.length + 2) + (B.length + 2)
                + (1 + lenC.length + 2) + (C.length + 2);

        ByteBuffer buf = ByteBuffer.allocate(cap);
        buf.put((byte) '*').put((byte) '3').put((byte) '\r').put((byte) '\n');

        buf.put((byte) '$').put(lenA).put((byte) '\r').put((byte) '\n');
        buf.put(A).put((byte) '\r').put((byte) '\n');

        buf.put((byte) '$').put(lenB).put((byte) '\r').put((byte) '\n');
        buf.put(B).put((byte) '\r').put((byte) '\n');

        buf.put((byte) '$').put(lenC).put((byte) '\r').put((byte) '\n');
        buf.put(C).put((byte) '\r').put((byte) '\n');

        buf.flip();
        return buf;
    }
}
