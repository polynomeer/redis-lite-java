package com.polynomeer.db;

import java.util.List;

/**
 * Minimal DB interface for single-threaded event loop usage.
 * All methods are expected to be called from the reactor thread only.
 */
public interface Db {

    /**
     * Get string value or null if not exists or wrong type.
     */
    String getString(String key);

    /**
     * Set string value with optional absolute expiration time (ms since epoch).
     * Use expireAtMs < 0 to store without TTL.
     */
    void setString(String key, String value, long expireAtMs);

    /**
     * Delete key; returns true if key existed.
     */
    boolean del(String key);

    /**
     * Returns true if key exists (and not expired).
     */
    boolean exists(String key);

    // ----- TTL processing -----

    /**
     * Expire keys that are due at or before nowMs.
     * Returns number of expired keys processed (best-effort).
     */
    int expireDue(long nowMs, int limit);

    /**
     * Milliseconds until the next expiration; negative if none scheduled.
     * Used by the reactor to set select() timeout.
     */
    long nextExpiryDelayMillis(long nowMs);

    // ----- Hash operations -----

    /**
     * HGET key field: returns value or null if missing.
     * Throws WrongTypeException if key holds a non-hash value.
     */
    String hget(String key, String field) throws WrongTypeException;

    /**
     * HSET key field value: returns 1 if a new field was created, 0 if field updated.
     * Creates the hash key if it does not exist.
     * Throws WrongTypeException if key holds a non-hash value.
     */
    int hset(String key, String field, String value) throws WrongTypeException;

    /**
     * HDEL key field [field ...]: returns number of fields removed.
     * If hash becomes empty after deletion, the key is removed.
     * Throws WrongTypeException if key holds a non-hash value.
     */
    int hdel(String key, List<String> fields) throws WrongTypeException;

    /**
     * Set absolute TTL: now + ms (ms>0), returns 1 if updated or key deleted when ms<=0, 0 if key missing.
     */
    int pexpire(String key, long ms);

    /**
     * Remaining TTL in ms: -2 if key doesn't exist, -1 if no TTL, else >=0.
     */
    long pttl(String key);
}
