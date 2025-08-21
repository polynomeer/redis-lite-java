package com.polynomeer.resp;

/**
 * RESP data types supported here (subset sufficient for commands).
 */
public enum RespType {
    SIMPLE_STRING,  // +OK\r\n
    ERROR,          // -ERR msg\r\n
    INTEGER,        // :123\r\n
    BULK_STRING,    // $3\r\nGET\r\n
    ARRAY,          // *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
    NULL_BULK,      // $-1\r\n
    NULL_ARRAY      // *-1\r\n
}
