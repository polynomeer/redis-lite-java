package com.polynomeer.resp;

/**
 * Lightweight exception for RESP parsing/writing errors.
 */
public class RespError extends RuntimeException {
    public RespError(String msg) {
        super(msg);
    }
}
