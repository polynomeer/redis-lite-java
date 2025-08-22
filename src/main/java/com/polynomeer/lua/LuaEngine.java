package com.polynomeer.lua;

import com.polynomeer.db.Db;
import com.polynomeer.pubsub.PubSubBroker;
import com.polynomeer.util.Sha1;
import org.luaj.vm2.*;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.*;

/**
 * Lua sandbox powered by luaj:
 * - Minimal libs: base, table, string, math (no io/os)
 * - Exposes table 'redis' with function redis.call(...)
 * - Cooperative time & resource limits enforced at each redis.call boundary
 */
public final class LuaEngine {

    public static class NoScript extends RuntimeException {
    }

    private final Db db;
    private final PubSubBroker broker;

    // Limits
    private final long timeLimitMs;
    private final int maxBytes;    // total bytes across args to redis.call
    private final int maxCalls;    // total redis.call invocations per script

    private final java.util.Map<String, String> cache = new java.util.HashMap<>();

    public LuaEngine(Db db, PubSubBroker broker, long timeLimitMs, int maxBytes, int maxCalls) {
        this.db = db;
        this.broker = broker;
        this.timeLimitMs = timeLimitMs;
        this.maxBytes = maxBytes;
        this.maxCalls = maxCalls;
    }

    public LuaValue eval(String script, java.util.List<String> keys, java.util.List<String> args) {
        String sha = Sha1.hex(script);
        cache.putIfAbsent(sha.toLowerCase(), script);
        return run(script, keys, args); // compile on current Globals
    }

    public LuaValue evalSha(String sha, java.util.List<String> keys, java.util.List<String> args) {
        String src = cache.get(sha.toLowerCase());
        if (src == null) throw new NoScript();
        return run(src, keys, args);
    }

    public String scriptLoad(String script) {
        String sha = Sha1.hex(script);
        cache.putIfAbsent(sha.toLowerCase(), script);
        return sha;
    }

    public boolean scriptExists(String sha) {
        return cache.containsKey(sha.toLowerCase());
    }

    public void scriptFlush() {
        cache.clear();
    }

    private LuaValue run(String script, java.util.List<String> keys, java.util.List<String> args) {
        Globals g = makeGlobals();

        // KEYS / ARGV
        LuaTable KEYS = new LuaTable();
        for (int i = 0; i < keys.size(); i++) KEYS.set(i + 1, LuaValue.valueOf(keys.get(i)));
        g.set("KEYS", KEYS);

        LuaTable ARGV = new LuaTable();
        for (int i = 0; i < args.size(); i++) ARGV.set(i + 1, LuaValue.valueOf(args.get(i)));
        g.set("ARGV", ARGV);

        // redis.call / redis.pcall
        Limits limits = new Limits(System.currentTimeMillis(), timeLimitMs, maxBytes, maxCalls);
        LuaTable redis = new LuaTable();
        redis.set("call", new RedisCall(limits));
        redis.set("pcall", new RedisPCall(limits));
        g.set("redis", redis);

        try {
            // ✅ load chunk in THIS Globals and get a function
            LuaFunction fn = g.load(script, "script").checkfunction();
            return fn.invoke(LuaValue.NONE).arg1();
        } catch (LuaError e) {
            throw new RuntimeException("Lua script execution failed: " + e.getMessage(), e);
        }
    }


    private Globals makeGlobals() {
        Globals g = new Globals();

        // Enable minimal libraries
        g.load(new BaseLib());
        g.load(new PackageLib());
        g.load(new StringLib());
        g.load(new TableLib());
        g.load(new MathLib());

        // Enable the LuaJ compiler to allow runtime script compilation
        LoadState.install(g);
        LuaC.install(g);

        return g;
    }

    // Cooperative limits context
    private static final class Limits {
        final long startMs;
        final long maxMs;
        final int maxBytes;
        final int maxCalls;
        int usedBytes = 0;
        int usedCalls = 0;

        Limits(long startMs, long maxMs, int maxBytes, int maxCalls) {
            this.startMs = startMs;
            this.maxMs = maxMs;
            this.maxBytes = maxBytes;
            this.maxCalls = maxCalls;
        }

        void tick(java.util.List<String> argv) {
            long now = System.currentTimeMillis();
            if (now - startMs > maxMs) throw new RuntimeException("Lua script timed out");
            int b = 0;
            for (String s : argv) b += s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            usedBytes += b;
            usedCalls++;
            if (usedBytes > maxBytes) throw new RuntimeException("Lua script exceeded call bytes limit");
            if (usedCalls > maxCalls) throw new RuntimeException("Lua script exceeded call count limit");
        }
    }

    // redis.call(...)
    private class RedisCall extends VarArgFunction {
        private final Limits lim;

        RedisCall(Limits lim) {
            this.lim = lim;
        }

        @Override
        public Varargs invoke(Varargs va) {
            // Convert Lua arguments to a Java list
            java.util.List<String> argv = toArgv(va);
            System.out.println("DEBUG redis.call arguments: " + argv);

            // Apply resource limits
            lim.tick(argv);

            // Extract and execute the command
            String cmd = argv.get(0).toUpperCase();
            System.out.println("DEBUG redis.call executing command: " + cmd);

            switch (cmd) {
                case "GET":
                    return v(get1(argv));
                case "SET":
                    return v(set(argv));
                case "DEL":
                    return LuaValue.valueOf(del(argv));
                case "EXISTS":
                    return LuaValue.valueOf(exists(argv));
                case "HGET":
                    return v(hget(argv));
                case "HSET":
                    return LuaValue.valueOf(hset(argv));
                case "PEXPIRE":
                    return LuaValue.valueOf(pexpire(argv));
                case "PTTL":
                    return LuaValue.valueOf(pttl(argv));
                case "PUBLISH":
                    return LuaValue.valueOf(publish(argv));
                default:
                    // Throw meaningful error for unsupported commands
                    throw new RuntimeException("Unsupported redis.call command: " + cmd);
            }
        }

        private String get1(java.util.List<String> a) {
            if (a.size() != 2) throw new RuntimeException("wrong number of arguments for 'GET'");
            return db.getString(a.get(1));
        }

        private String set(java.util.List<String> a) {
            if (a.size() < 3) throw new RuntimeException("wrong number of arguments for 'SET'");
            String key = a.get(1), val = a.get(2);
            boolean nx = false, xx = false;
            Long pxMs = null;
            for (int i = 3; i < a.size(); i++) {
                String opt = a.get(i).toUpperCase();
                switch (opt) {
                    case "NX":
                        nx = true;
                        break;
                    case "XX":
                        xx = true;
                        break;
                    case "PX":
                        if (++i >= a.size()) throw new RuntimeException("syntax error");
                        pxMs = parsePosLong(a.get(i));
                        break;
                    case "EX":
                        if (++i >= a.size()) throw new RuntimeException("syntax error");
                        pxMs = parsePosLong(a.get(i)) * 1000L;
                        break;
                    default:
                        throw new RuntimeException("syntax error");
                }
            }
            boolean exists = db.exists(key);
            if (nx && exists) return null;
            if (xx && !exists) return null;
            long expireAt = (pxMs == null) ? -1L : (System.currentTimeMillis() + pxMs);
            db.setString(key, val, expireAt);
            return "OK";
        }

        private int del(java.util.List<String> a) {
            if (a.size() < 2) throw new RuntimeException("wrong number of arguments for 'DEL'");
            int n = 0;
            for (int i = 1; i < a.size(); i++) if (db.del(a.get(i))) n++;
            return n;
        }

        private int exists(java.util.List<String> a) {
            if (a.size() != 2) throw new RuntimeException("wrong number of arguments for 'EXISTS'");
            return db.exists(a.get(1)) ? 1 : 0;
        }

        private String hget(java.util.List<String> a) {
            if (a.size() != 3) throw new RuntimeException("wrong number of arguments for 'HGET'");
            try {
                return db.hget(a.get(1), a.get(2));
            } catch (RuntimeException e) {
                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        }

        private int hset(java.util.List<String> a) {
            if (a.size() != 4) throw new RuntimeException("wrong number of arguments for 'HSET'");
            try {
                return db.hset(a.get(1), a.get(2), a.get(3));
            } catch (RuntimeException e) {
                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        }

        private int hdel(java.util.List<String> a) {
            if (a.size() < 3) throw new RuntimeException("wrong number of arguments for 'HDEL'");
            try {
                return db.hdel(a.get(1), a.subList(2, a.size()));
            } catch (RuntimeException e) {
                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        }

        private int pexpire(java.util.List<String> a) {
            if (a.size() != 3) throw new RuntimeException("wrong number of arguments for 'PEXPIRE'");
            long ms = parsePosLong(a.get(2));
            return db.pexpire(a.get(1), ms);
        }

        private long pttl(java.util.List<String> a) {
            if (a.size() != 2) throw new RuntimeException("wrong number of arguments for 'PTTL'");
            return db.pttl(a.get(1));
        }

        private int publish(java.util.List<String> a) {
            if (a.size() != 3) throw new RuntimeException("wrong number of arguments for 'PUBLISH'");
            return broker.publish(a.get(1), a.get(2));
        }

        private long parsePosLong(String s) {
            long v = Long.parseLong(s);
            if (v <= 0) throw new RuntimeException("value is out of range");
            return v;
        }


        // Helper function to convert Lua values to Java strings
        private java.util.List<String> toArgv(Varargs va) {
            int n = va.narg();
            java.util.List<String> out = new java.util.ArrayList<>(n);
            for (int i = 1; i <= n; i++) out.add(va.arg(i).tojstring());
            return out;
        }

        // Debug helper for null-safe return
        private LuaValue v(String s) {
            return (s == null) ? LuaValue.NIL : LuaValue.valueOf(s);
        }

    }

    // redis.pcall(...) -> like call but returns ('err') as lua error string instead of throwing
    private final class RedisPCall extends RedisCall {
        RedisPCall(Limits lim) {
            super(lim);
        }

        @Override
        public Varargs invoke(Varargs va) {
            try {
                return super.invoke(va);
            } catch (RuntimeException e) {
                return LuaValue.varargsOf(LuaValue.NIL, LuaValue.valueOf("ERR " + e.getMessage()));
            }
        }
    }
}
