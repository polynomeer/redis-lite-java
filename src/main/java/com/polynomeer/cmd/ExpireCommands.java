package com.polynomeer.cmd;

import com.polynomeer.db.Db;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Expiration commands: PEXPIRE key ms, PTTL key
 */
public final class ExpireCommands {
    private ExpireCommands() {
    }

    public static void register(Map<String, Command> reg, Db db) {
        reg.put("PEXPIRE", (argv, ctx) -> pexpire(db, argv));
        reg.put("PTTL", (argv, ctx) -> pttl(db, argv));
    }

    private static ByteBuffer pexpire(Db db, List<String> argv) {
        if (argv.size() != 3) return RespWriter.error("ERR wrong number of arguments for 'PEXPIRE'");
        String key = argv.get(1);
        long ms;
        try {
            ms = Long.parseLong(argv.get(2));
        } catch (Exception e) {
            return RespWriter.error("ERR value is not an integer or out of range");
        }
        if (ms <= 0) {
            // Redis: ms <= 0 → key is expired (deleted) and returns 1 if existed.
            int r = db.pexpire(key, ms);
            return RespWriter.integer(r);
        }
        return RespWriter.integer(db.pexpire(key, ms));
    }

    private static ByteBuffer pttl(Db db, List<String> argv) {
        if (argv.size() != 2) return RespWriter.error("ERR wrong number of arguments for 'PTTL'");
        long v = db.pttl(argv.get(1));
        return RespWriter.integer(v);
    }
}
