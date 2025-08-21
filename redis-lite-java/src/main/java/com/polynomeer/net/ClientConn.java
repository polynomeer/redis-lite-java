package com.polynomeer.net;

import com.polynomeer.cmd.PingEchoCommands;
import com.polynomeer.resp.RespReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Per-connection state: read buffer, write queue, parsing cursor.
 * - OP_READ: accumulate bytes, parse RESP requests (pipelined)
 * - Execute command (PING/ECHO stub) and enqueue RESP response
 * - OP_WRITE: flush write queue with partial write handling
 */
public class ClientConn {
    private static final int READ_BUF_SIZE = 64 * 1024;

    private final SocketChannel ch;
    private final Selector selector;

    private final ByteBuffer readBuf = ByteBuffer.allocate(READ_BUF_SIZE);
    private final Deque<ByteBuffer> writeQueue = new ArrayDeque<>();

    private final RespReader respReader = new RespReader();

    public ClientConn(SocketChannel ch, Selector selector) {
        this.ch = ch;
        this.selector = selector;
    }

    /**
     * Handle readable event: read → parse → execute → queue responses
     */
    public void handleRead() throws IOException {
        int n = ch.read(readBuf);
        if (n == -1) {
            closeQuietly();
            return;
        }
        if (n == 0) return;

        readBuf.flip(); // switch to read mode for parser

        // Parse as many RESP Array(Bulk Strings) as available (pipelining)
        while (true) {
            int markPos = readBuf.position();
            List<String> argv = respReader.tryReadCommand(readBuf);
            if (argv == null) {
                // Not enough bytes for a full frame → rewind and stop
                readBuf.position(markPos);
                break;
            }

            // Dispatch minimal commands (PING/ECHO); unknown → -ERR
            ByteBuffer resp = PingEchoCommands.dispatch(argv);
            enqueue(resp);
        }

        // Compact buffer to keep any unconsumed bytes at beginning
        readBuf.compact();

        // Ensure OP_WRITE is enabled if we have pending data
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
                // Kernel buffer full: wait for next OP_WRITE
                break;
            }
            writeQueue.pollFirst();
        }

        // If nothing left to write, disable OP_WRITE to avoid busy-loop
        if (writeQueue.isEmpty()) {
            SelectionKey key = ch.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        }
    }

    private void enqueue(ByteBuffer response) {
        // Response ByteBuffer must be in read mode (position at 0)
        writeQueue.addLast(response);
    }

    public void closeQuietly() {
        try {
            ch.close();
        } catch (Exception ignore) {
        }
    }
}
