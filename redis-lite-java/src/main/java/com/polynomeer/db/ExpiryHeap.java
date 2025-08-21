package com.polynomeer.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple min-heap of (key, expireAtMs).
 * - We keep it append-only; on pop we validate against current DB state.
 * - If the key was deleted or TTL changed, the popped entry is ignored.
 */
final class ExpiryHeap {

    static final class Node {
        final String key;
        final long expireAtMs;

        Node(String key, long expireAtMs) {
            this.key = key;
            this.expireAtMs = expireAtMs;
        }
    }

    private final List<Node> heap = new ArrayList<>();

    boolean isEmpty() {
        return heap.isEmpty();
    }

    void push(String key, long expireAtMs) {
        heap.add(new Node(key, expireAtMs));
        siftUp(heap.size() - 1);
    }

    Node peek() {
        return heap.isEmpty() ? null : heap.get(0);
    }

    Node pop() {
        if (heap.isEmpty()) return null;
        Node top = heap.get(0);
        Node last = heap.remove(heap.size() - 1);
        if (!heap.isEmpty()) {
            heap.set(0, last);
            siftDown(0);
        }
        return top;
    }

    private void siftUp(int i) {
        while (i > 0) {
            int p = (i - 1) >>> 1;
            if (heap.get(p).expireAtMs <= heap.get(i).expireAtMs) break;
            swap(p, i);
            i = p;
        }
    }

    private void siftDown(int i) {
        int n = heap.size();
        while (true) {
            int l = (i << 1) + 1, r = l + 1, m = i;
            if (l < n && heap.get(l).expireAtMs < heap.get(m).expireAtMs) m = l;
            if (r < n && heap.get(r).expireAtMs < heap.get(m).expireAtMs) m = r;
            if (m == i) break;
            swap(i, m);
            i = m;
        }
    }

    private void swap(int a, int b) {
        Node t = heap.get(a);
        heap.set(a, heap.get(b));
        heap.set(b, t);
    }
}
