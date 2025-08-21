package com.polynomeer.net;

import com.polynomeer.cmd.CommandRegistry;
import com.polynomeer.pubsub.PubSubBroker;
import com.polynomeer.resp.RespReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * Per-connection state:
 * - read buffer, write queue, parsing cursor
 * - subscription set for Pub/Sub
 * OP_READ: parse RESP command frames (pipelined) → dispatch → enqueue replies
 * OP_WRITE: flush write queue (partial writes)
 */
public class ClientConn {
    private static final int READ_BUF_SIZE = 64 * 1024;

    private final SocketChannel ch;
    private final Selector selector;
    private final PubSubBroker broker;

    private final ByteBuffer readBuf = ByteBuffer.allocate(READ_BUF_SIZE);
    private final Deque<ByteBuffer> writeQueue = new ArrayDeque<>();
    private final RespReader respReader = new RespReader();

    // The set of channels this connection subscribed to
    private final Set<String> subscriptions = new HashSet<>();

    public ClientConn(SocketChannel ch, Selector selector, PubSubBroker broker) {
        this.ch = ch;
        this.selector = selector;
        this.broker = broker;
    }

    /**
     * Handle readable event: read → parse → execute → queue responses
     */
    public void handleRead() throws IOException {
        int n = ch.read(readBuf);
        if (n == -1) {
            onDisconnect();
            closeQuietly();
            return;
        }
        if (n == 0) return;

        readBuf.flip();

        while (true) {
            int markPos = readBuf.position();
            List<String> argv = respReader.tryReadCommand(readBuf);
            if (argv == null) {
                readBuf.position(markPos);
                break;
            }
            ByteBuffer resp = CommandRegistry.dispatch(argv, this);
            if (resp != null) {
                enqueue(resp);
            }
        }

        readBuf.compact();

        if (!writeQueue.isEmpty()) {
            SelectionKey key = ch.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Handle writable event: flush write queue with partial-write care
     */
    public void handleWrite() throws IOException {
        while (!writeQueue.isEmpty()) {
            ByteBuffer buf = writeQueue.peekFirst();
            ch.write(buf);
            if (buf.hasRemaining()) {
                break;
            }
            writeQueue.pollFirst();
        }
        if (writeQueue.isEmpty()) {
            SelectionKey key = ch.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Enqueue a server-pushed message (e.g., Pub/Sub) and ensure OP_WRITE is enabled.
     */
    public void push(ByteBuffer response) {
        enqueue(response);
        SelectionKey key = ch.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void enqueue(ByteBuffer response) {
        writeQueue.addLast(response);
    }

    public void closeQuietly() {
        try {
            ch.close();
        } catch (Exception ignore) {
        }
    }

    /**
     * On disconnect, unsubscribe from all channels.
     */
    public void onDisconnect() {
        if (!subscriptions.isEmpty()) {
            broker.unsubscribeAll(this, subscriptions);
            subscriptions.clear();
        }
    }

    // ---- subscription helpers (used by commands/broker) ----

    public void addSubscription(String channel) {
        subscriptions.add(channel);
    }

    public boolean removeSubscription(String channel) {
        return subscriptions.remove(channel);
    }

    public int subscriptionCount() {
        return subscriptions.size();
    }
}
