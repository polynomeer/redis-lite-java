package com.polynomeer.net;

import com.polynomeer.cmd.CommandRegistry;
import com.polynomeer.db.Db;
import com.polynomeer.db.MemoryDb;
import com.polynomeer.lua.LuaEngine;
import com.polynomeer.pubsub.PubSubBroker;
import com.polynomeer.util.Clocks;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class Reactor {
    private final int port;
    private Selector selector;
    private ServerSocketChannel server;
    private final Db db;
    private final PubSubBroker broker;
    private final LuaEngine lua;

    private static final int EXPIRE_BATCH_LIMIT = 2000;

    public Reactor(int port) {
        this.port = port;
        this.db = new MemoryDb(); // single DB (DB 0)
        this.broker = new PubSubBroker();
        // Lua sandbox limits: 5_000 ms, max 10_000 redis.call bytes, max 1_000 calls
        this.lua = new LuaEngine(db, broker, 5_000L, 10_000, 1_000);
        CommandRegistry.initDefaults(db, broker, lua);
    }

    public void start() throws IOException {
        selector = Selector.open();

        server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(port));
        server.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("[jredis] Listening on port " + port);

        while (true) {
            long nowMs = Clocks.monoMillis();
            long delayMs = db.nextExpiryDelayMillis(nowMs);
            if (delayMs < 0) delayMs = 1000; // no expirations known

            selector.select(Math.max(1, Math.min(delayMs, 1000)));

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
                        ((ClientConn) att).onDisconnect(); // unsubscribe all
                        ((ClientConn) att).closeQuietly();
                    }
                    key.cancel();
                }
            }

            nowMs = Clocks.monoMillis();
            db.expireDue(nowMs, EXPIRE_BATCH_LIMIT);
        }
    }

    private void handleAccept() throws IOException {
        SocketChannel ch = server.accept();
        if (ch == null) return;
        ch.configureBlocking(false);
        ClientConn conn = new ClientConn(ch, selector, broker);
        ch.register(selector, SelectionKey.OP_READ, conn);
        System.out.println("[jredis] Accepted " + ch.getRemoteAddress());
    }
}
