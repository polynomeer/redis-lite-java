package com.polynomeer.db;

/**
 * Signals an operation against a key holding the wrong kind of value.
 */
public class WrongTypeException extends RuntimeException {
    public WrongTypeException() {
        super();
    }

    public WrongTypeException(String msg) {
        super(msg);
    }
}
