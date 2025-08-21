package com.polynomeer.db;

import com.polynomeer.struct.OpenHashStringMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single-threaded in-memory keyspace with millisecond TTL support.
 * - Passive expiration on access (get/exist/del)
 * - Active expiration via ExpiryHeap popped in reactor loop
 * - Hash keys use an internal open-addressing string map
 * <p>
 * Note: Designed for reactor-thread-only access (no synchronization).
 */
public class MemoryDb implements Db {

    private final Map<String, Record> map = new HashMap<>();
    private final ExpiryHeap heap = new ExpiryHeap();

    @Override
    public String getString(String key) {
        Record r = map.get(key);
        if (r == null) return null;
        if (isExpired(r, System.currentTimeMillis())) {
            map.remove(key);
            return null;
        }
        return r.type == Record.Type.STR ? r.strVal : null;
    }

    @Override
    public void setString(String key, String value, long expireAtMs) {
        Record r = new Record(value, expireAtMs);
        map.put(key, r);
        if (r.hasTtl()) {
            heap.push(key, r.expireAtMs);
        }
    }

    @Override
    public boolean del(String key) {
        Record r = map.remove(key);
        return r != null;
    }

    @Override
    public boolean exists(String key) {
        Record r = map.get(key);
        if (r == null) return false;
        if (isExpired(r, System.currentTimeMillis())) {
            map.remove(key);
            return false;
        }
        return true;
    }

    @Override
    public int expireDue(long nowMs, int limit) {
        int n = 0;
        while (n < limit) {
            ExpiryHeap.Node top = heap.peek();
            if (top == null || top.expireAtMs > nowMs) break;

            heap.pop();
            Record r = map.get(top.key);
            if (r == null) continue; // already gone
            if (r.expireAtMs == top.expireAtMs && r.expireAtMs <= nowMs) {
                map.remove(top.key);
                n++;
            }
        }
        return n;
    }

    @Override
    public long nextExpiryDelayMillis(long nowMs) {
        ExpiryHeap.Node top = heap.peek();
        if (top == null) return -1;
        long d = top.expireAtMs - nowMs;
        return Math.max(0, d);
    }

    // ---------- Hash operations ----------

    @Override
    public String hget(String key, String field) throws WrongTypeException {
        Record r = map.get(key);
        if (r == null) return null;
        if (isExpired(r, System.currentTimeMillis())) {
            map.remove(key);
            return null;
        }
        if (r.type != Record.Type.HASH) {
            throw new WrongTypeException();
        }
        return r.hashVal.get(field);
    }

    @Override
    public int hset(String key, String field, String value) throws WrongTypeException {
        Record r = map.get(key);
        long now = System.currentTimeMillis();

        if (r == null || isExpired(r, now)) {
            // Create new hash record without TTL by default
            OpenHashStringMap m = new OpenHashStringMap();
            r = new Record(m, -1);
            map.put(key, r);
        } else if (r.type != Record.Type.HASH) {
            throw new WrongTypeException();
        }
        boolean isNew = r.hashVal.put(field, value);
        // TTL unchanged (as in Redis)
        return isNew ? 1 : 0;
    }

    @Override
    public int hdel(String key, List<String> fields) throws WrongTypeException {
        Record r = map.get(key);
        if (r == null) return 0;
        if (isExpired(r, System.currentTimeMillis())) {
            map.remove(key);
            return 0;
        }
        if (r.type != Record.Type.HASH) {
            throw new WrongTypeException();
        }
        int removed = 0;
        for (String f : fields) {
            if (r.hashVal.remove(f)) removed++;
        }
        // If hash becomes empty, remove the key
        if (r.hashVal.size() == 0) {
            map.remove(key);
        }
        return removed;
    }

    // ---------- helpers ----------

    private boolean isExpired(Record r, long nowMs) {
        return r.hasTtl() && r.expireAtMs <= nowMs;
    }
}
