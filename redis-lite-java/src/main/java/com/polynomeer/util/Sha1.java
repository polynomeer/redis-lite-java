package com.polynomeer.util;

import java.security.MessageDigest;

/**
 * SHA-1 hex helper for SCRIPT cache keys.
 */
public final class Sha1 {
    private Sha1() {
    }

    public static String hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] d = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(d.length * 2);
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
