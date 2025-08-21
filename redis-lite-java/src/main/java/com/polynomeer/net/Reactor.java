package com.polynomeer.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * Single-threaded Reactor using Selector.
 * - Accepts new clients
 * - Dispatches READ/WRITE to attached ClientConn
 */
public class Reactor {
    private final int port;
    private Selector selector;
    private ServerSocketChannel server;

    public Reactor(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        selector = Selector.open();

        server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.bind(new InetSocketAddress(port));
        server.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("[jredis] Listening on port " + port);

        while (true) {
            // The only blocking point in the event loop
            selector.select();

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
                    // On IO error: close connection
                    Object att = key.attachment();
                    if (att instanceof ClientConn) {
                        ((ClientConn) att).closeQuietly();
                    }
                    key.cancel();
                }
            }
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
