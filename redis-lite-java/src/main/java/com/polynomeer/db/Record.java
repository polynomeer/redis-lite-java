package com.polynomeer.db;

/**
 * Record stored in keyspace.
 * For step-1 we only support STR type; more types can be added later.
 */
final class Record {
    enum Type {STR}

    Type type = Type.STR;
    String strVal;

    // Absolute expiration time in millis since epoch; < 0 means no TTL
    long expireAtMs = -1L;

    // Heap bookkeeping: we avoid strict index tracking; instead we validate on pop.
    // (If you want O(log n) updates by key, add an index field and a heap map.)
    Record(String strVal, long expireAtMs) {
        this.strVal = strVal;
        this.expireAtMs = expireAtMs;
    }

    boolean hasTtl() {
        return expireAtMs >= 0;
    }
}
