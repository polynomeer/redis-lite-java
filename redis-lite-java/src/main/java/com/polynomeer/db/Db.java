package com.polynomeer.db;

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
}
