package com.polynomeer.struct;

/**
 * A lightweight open-addressing hash map specialized for String->String.
 * - Linear probing with states array (0=empty, 1=used, 2=deleted)
 * - Power-of-two capacity, load factor ~0.66 before rehash
 * - Not thread-safe; intended for single-threaded reactor usage
 */
public final class OpenHashStringMap {
    private static final int INIT_CAP = 8; // must be power of two
    private static final double MAX_LOAD = 0.66;

    private String[] keys;
    private String[] vals;
    private byte[] states; // 0=empty, 1=used, 2=deleted
    private int size;      // number of used entries
    private int occupied;  // used + deleted (for rehash trigger)

    public OpenHashStringMap() {
        this.keys = new String[INIT_CAP];
        this.vals = new String[INIT_CAP];
        this.states = new byte[INIT_CAP];
        this.size = 0;
        this.occupied = 0;
    }

    /**
     * Returns number of key/value pairs.
     */
    public int size() {
        return size;
    }

    /**
     * Get value or null if absent.
     */
    public String get(String k) {
        int cap = keys.length;
        int mask = cap - 1;
        int i = indexFor(k, mask);
        while (true) {
            byte st = states[i];
            if (st == 0) return null; // empty slot -> not found
            if (st == 1 && k.equals(keys[i])) {
                return vals[i];
            }
            i = (i + 1) & mask;
        }
    }

    /**
     * Put key/value.
     *
     * @return true if a new entry was added, false if updated existing.
     */
    public boolean put(String k, String v) {
        ensureCapacityForInsert();
        int cap = keys.length;
        int mask = cap - 1;
        int i = indexFor(k, mask);
        int firstTomb = -1;

        while (true) {
            byte st = states[i];
            if (st == 0) {
                // use tombstone if present, else this empty slot
                int idx = (firstTomb >= 0) ? firstTomb : i;
                if (states[idx] == 0) occupied++; // first occupation
                keys[idx] = k;
                vals[idx] = v;
                states[idx] = 1;
                size++;
                return true;
            } else if (st == 1) {
                if (k.equals(keys[i])) {
                    // update
                    vals[i] = v;
                    return false;
                }
            } else { // st == 2 (deleted)
                if (firstTomb < 0) firstTomb = i;
            }
            i = (i + 1) & mask;
        }
    }

    /**
     * Remove key; returns true if removed.
     */
    public boolean remove(String k) {
        int cap = keys.length;
        int mask = cap - 1;
        int i = indexFor(k, mask);
        while (true) {
            byte st = states[i];
            if (st == 0) return false; // not found
            if (st == 1 && k.equals(keys[i])) {
                // mark tombstone
                keys[i] = null;
                vals[i] = null;
                states[i] = 2;
                size--;
                // Optional: rehash down if too sparse (not critical for this step)
                if (cap > INIT_CAP && size * 4 < cap) {
                    rehash(cap >>> 1);
                }
                return true;
            }
            i = (i + 1) & mask;
        }
    }

    private void ensureCapacityForInsert() {
        int cap = keys.length;
        if ((occupied + 1) > (int) (cap * MAX_LOAD)) {
            rehash(cap << 1);
        }
    }

    private void rehash(int newCap) {
        newCap = Math.max(INIT_CAP, nextPowerOfTwo(newCap));
        String[] oldK = keys;
        String[] oldV = vals;
        byte[] oldS = states;

        keys = new String[newCap];
        vals = new String[newCap];
        states = new byte[newCap];
        size = 0;
        occupied = 0;

        int mask = newCap - 1;
        for (int i = 0; i < oldK.length; i++) {
            if (oldS[i] == 1) {
                // re-insert
                String k = oldK[i];
                String v = oldV[i];
                int j = indexFor(k, mask);
                while (true) {
                    byte st = states[j];
                    if (st == 0) {
                        keys[j] = k;
                        vals[j] = v;
                        states[j] = 1;
                        size++;
                        occupied++;
                        break;
                    }
                    j = (j + 1) & mask;
                }
            }
        }
    }

    private static int nextPowerOfTwo(int x) {
        int n = x - 1;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        return (n < 0) ? 1 : (n + 1);
    }

    private static int spread(int h) {
        // Hash spread similar to JDK HashMap
        return h ^ (h >>> 16);
    }

    private static int indexFor(String k, int mask) {
        return spread(k.hashCode()) & mask;
    }
}
