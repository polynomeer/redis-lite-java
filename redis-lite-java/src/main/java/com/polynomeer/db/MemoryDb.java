package com.polynomeer.db;

import java.util.HashMap;
import java.util.Map;

/**
 * Single-threaded in-memory keyspace with millisecond TTL support.
 * - Passive expiration on access (get/exist/del)
 * - Active expiration via ExpiryHeap popped in reactor loop
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
            // Lazy delete on access
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
            if (r == null) {
                // was deleted
                continue;
            }
            if (r.expireAtMs == top.expireAtMs && r.expireAtMs <= nowMs) {
                // still valid and due
                map.remove(top.key);
                n++;
            }
            // else: TTL changed, ignore this stale node
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

    private boolean isExpired(Record r, long nowMs) {
        return r.hasTtl() && r.expireAtMs <= nowMs;
    }
}
