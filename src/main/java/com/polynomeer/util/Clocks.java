package com.polynomeer.util;

/**
 * Time helpers; prefer monotonic time for scheduling.
 */
public final class Clocks {
    private Clocks() {
    }

    /**
     * Wall clock millis; suitable for absolute TTL timestamps.
     */
    public static long nowMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Monotonic milliseconds derived from nanoTime; use for relative delays.
     */
    public static long monoMillis() {
        return System.nanoTime() / 1_000_000L;
    }
}
