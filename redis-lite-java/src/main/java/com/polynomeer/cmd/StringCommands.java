package com.polynomeer.cmd;

import com.polynomeer.db.Db;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class StringCommands {
    private StringCommands() {
    }

    public static void register(Map<String, Command> reg, Db db) {
        reg.put("GET", (argv, ctx) -> get(db, argv));
        reg.put("SET", (argv, ctx) -> set(db, argv));
        reg.put("DEL", (argv, ctx) -> del(db, argv));
    }

    private static ByteBuffer get(Db db, List<String> argv) {
        if (argv.size() != 2) return RespWriter.error("ERR wrong number of arguments for 'GET'");
        String key = argv.get(1);
        String val = db.getString(key);
        if (val == null) return RespWriter.nullBulk();
        return RespWriter.bulkString(val);
    }

    private static ByteBuffer set(Db db, List<String> argv) {
        if (argv.size() < 3) return RespWriter.error("ERR wrong number of arguments for 'SET'");
        String key = argv.get(1);
        String value = argv.get(2);

        boolean nx = false, xx = false;
        Long pxMs = null;

        int i = 3;
        while (i < argv.size()) {
            String opt = argv.get(i).toUpperCase(Locale.ROOT);
            switch (opt) {
                case "PX":
                    if (i + 1 >= argv.size()) return RespWriter.error("ERR syntax error");
                    pxMs = parsePositiveLong(argv.get(++i), "PX");
                    break;
                case "EX":
                    if (i + 1 >= argv.size()) return RespWriter.error("ERR syntax error");
                    long sec = parsePositiveLong(argv.get(++i), "EX");
                    pxMs = sec * 1000L;
                    break;
                case "NX":
                    nx = true;
                    break;
                case "XX":
                    xx = true;
                    break;
                default:
                    return RespWriter.error("ERR syntax error");
            }
            i++;
        }
        if (nx && xx) return RespWriter.error("ERR NX and XX options at the same time are not compatible");

        boolean exists = db.exists(key);
        if (nx && exists) return RespWriter.nullBulk();
        if (xx && !exists) return RespWriter.nullBulk();

        long expireAt = (pxMs == null) ? -1L : (System.currentTimeMillis() + pxMs);
        db.setString(key, value, expireAt);
        return RespWriter.simpleString("OK");
    }

    private static ByteBuffer del(Db db, List<String> argv) {
        if (argv.size() < 2) return RespWriter.error("ERR wrong number of arguments for 'DEL'");
        long deleted = 0;
        for (int i = 1; i < argv.size(); i++) {
            if (db.del(argv.get(i))) deleted++;
        }
        return RespWriter.integer(deleted);
    }

    private static long parsePositiveLong(String s, String opt) {
        try {
            long v = Long.parseLong(s);
            if (v <= 0) throw new IllegalArgumentException();
            return v;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid " + opt + " value: " + s);
        }
    }
}
