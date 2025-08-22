package com.polynomeer.db;

import com.polynomeer.struct.OpenHashStringMap;

/**
 * Record stored in keyspace.
 * Types supported so far: STR, HASH
 */
final class Record {
    enum Type {STR, HASH}

    Type type;
    String strVal;                // when type == STR
    OpenHashStringMap hashVal;    // when type == HASH

    // Absolute expiration time in millis since epoch; < 0 means no TTL
    long expireAtMs = -1L;

    Record(String strVal, long expireAtMs) {
        this.type = Type.STR;
        this.strVal = strVal;
        this.expireAtMs = expireAtMs;
    }

    Record(OpenHashStringMap map, long expireAtMs) {
        this.type = Type.HASH;
        this.hashVal = map;
        this.expireAtMs = expireAtMs;
    }

    boolean hasTtl() {
        return expireAtMs >= 0;
    }
}
