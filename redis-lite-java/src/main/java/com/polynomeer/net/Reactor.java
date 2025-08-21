package com.polynomeer.net;

import com.polynomeer.cmd.CommandRegistry;
import com.polynomeer.db.Db;
import com.polynomeer.db.MemoryDb;
import com.polynomeer.util.Clocks;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Single-threaded Reactor using Selector.
 * Now integrates with in-memory DB and TTL expiration scheduling.
 */
public class Reactor {
    private final int port;
    private Selector selector;
    private ServerSocketChannel server;
    private final Db db;

    // Expiration batch limit per loop to avoid long stalls
    private static final int EXPIRE_BATCH_LIMIT = 2000;

    public Reactor(int port) {
        this.port = port;
        this.db = new MemoryDb(); // single DB (DB 0)
        CommandRegistry.initDefaults(db); // register commands against this DB
    }

    public void start() throws IOException {
        selector = Selector.open();

        server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(port));
        server.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("[jredis] Listening on port " + port);

        while (true) {
            // Compute select timeout based on nearest key expiration.
            long nowMs = Clocks.monoMillis();
            long delayMs = db.nextExpiryDelayMillis(nowMs);
            if (delayMs < 0) delayMs = 1000; // no expirations known

            // The only blocking point in the event loop
            selector.select(Math.max(1, Math.min(delayMs, 1000)));

            // Process IO events
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();

                if (!key.isValid()) continue;

                try {
                    if (key.isAcceptable()) {
                        handleAccept();
                    } else if (key.isReadable()) {
                        ((ClientConn) key.attachment()).handleRead();
                    } else if (key.isWritable()) {
                        ((ClientConn) key.attachment()).handleWrite();
                    }
                } catch (IOException e) {
                    Object att = key.attachment();
                    if (att instanceof ClientConn) {
                        ((ClientConn) att).closeQuietly();
                    }
                    key.cancel();
                }
            }

            // Run expiration after handling IO to keep latency low
            nowMs = Clocks.monoMillis();
            db.expireDue(nowMs, EXPIRE_BATCH_LIMIT);
        }
    }

    private void handleAccept() throws IOException {
        SocketChannel ch = server.accept();
        if (ch == null) return; // spurious
        ch.configureBlocking(false);
        ClientConn conn = new ClientConn(ch, selector);
        ch.register(selector, SelectionKey.OP_READ, conn);
        System.out.println("[jredis] Accepted " + ch.getRemoteAddress());
    }
}

