package com.polynomeer.cmd;

import com.polynomeer.db.Db;
import com.polynomeer.db.WrongTypeException;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class HashCommands {
    private HashCommands() {
    }

    public static void register(Map<String, Command> reg, Db db) {
        reg.put("HSET", (argv, ctx) -> hset(db, argv));
        reg.put("HGET", (argv, ctx) -> hget(db, argv));
        reg.put("HDEL", (argv, ctx) -> hdel(db, argv));
    }

    private static ByteBuffer hset(Db db, List<String> argv) {
        if (argv.size() < 4 || (argv.size() % 2) != 0) {
            return RespWriter.error("ERR wrong number of arguments for 'HSET'");
        }
        String key = argv.get(1);
        int added = 0;
        try {
            for (int i = 2; i < argv.size(); i += 2) {
                String field = argv.get(i);
                String value = argv.get(i + 1);
                added += db.hset(key, field, value);
            }
        } catch (WrongTypeException e) {
            return RespWriter.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return RespWriter.integer(added);
    }

    private static ByteBuffer hget(Db db, List<String> argv) {
        if (argv.size() != 3) {
            return RespWriter.error("ERR wrong number of arguments for 'HGET'");
        }
        String key = argv.get(1);
        String field = argv.get(2);
        String val;
        try {
            val = db.hget(key, field);
        } catch (WrongTypeException e) {
            return RespWriter.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return (val == null) ? RespWriter.nullBulk() : RespWriter.bulkString(val);
    }

    private static ByteBuffer hdel(Db db, List<String> argv) {
        if (argv.size() < 3) {
            return RespWriter.error("ERR wrong number of arguments for 'HDEL'");
        }
        String key = argv.get(1);
        List<String> fields = new ArrayList<>(argv.subList(2, argv.size()));
        int removed;
        try {
            removed = db.hdel(key, fields);
        } catch (WrongTypeException e) {
            return RespWriter.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return RespWriter.integer(removed);
    }
}
