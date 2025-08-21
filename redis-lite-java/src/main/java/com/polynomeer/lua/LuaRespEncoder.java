package com.polynomeer.lua;

import org.luaj.vm2.*;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Encodes a LuaValue into RESP:
 * - nil           -> $-1
 * - boolean       -> :1 / :0
 * - number        -> :<int>  (floats truncated)
 * - string        -> $<len>\r\n...\r\n
 * - table (array) -> *N + elements (1..N); non-integer keys ignored
 */
public final class LuaRespEncoder {
    private LuaRespEncoder() {
    }

    public static ByteBuffer encode(LuaValue v) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(256);
        write(out, v);
        byte[] bytes = out.toByteArray();
        return ByteBuffer.wrap(bytes);
    }

    private static void write(ByteArrayOutputStream out, LuaValue v) {
        if (v.isnil()) {
            out.writeBytes("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        } else if (v.isboolean()) {
            out.writeBytes((v.toboolean() ? ":1\r\n" : ":0\r\n").getBytes(StandardCharsets.UTF_8));
        } else if (v.isnumber()) {
            String s = ":" + Long.toString(v.tolong()) + "\r\n";
            out.writeBytes(s.getBytes(StandardCharsets.UTF_8));
        } else if (v.isstring()) {
            byte[] b = v.tojstring().getBytes(StandardCharsets.UTF_8);
            String h = "$" + b.length + "\r\n";
            out.writeBytes(h.getBytes(StandardCharsets.UTF_8));
            out.writeBytes(b);
            out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        } else if (v.istable()) {
            LuaTable t = v.checktable();
            int n = t.length();
            String h = "*" + n + "\r\n";
            out.writeBytes(h.getBytes(StandardCharsets.UTF_8));
            for (int i = 1; i <= n; i++) {
                write(out, t.get(i));
            }
        } else {
            // Fallback: encode as string via tostring()
            byte[] b = v.tojstring().getBytes(StandardCharsets.UTF_8);
            String h = "$" + b.length + "\r\n";
            out.writeBytes(h.getBytes(StandardCharsets.UTF_8));
            out.writeBytes(b);
            out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        }
    }
}
