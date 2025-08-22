package com.polynomeer.cmd;

import com.polynomeer.net.ClientConn;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Command interface with execution context (ClientConn) for operations
 * that need to push messages (e.g., SUBSCRIBE/UNSUBSCRIBE).
 * Return:
 * - ByteBuffer: immediate response to enqueue by caller, or
 * - null: command already enqueued its own responses (e.g., SUBSCRIBE multi-acks)
 */
public interface Command {
    ByteBuffer execute(List<String> argv, ClientConn ctx);
}
