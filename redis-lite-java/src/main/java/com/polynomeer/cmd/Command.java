package com.polynomeer.cmd;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Command interface if you later want a registry of many commands.
 */
public interface Command {
    ByteBuffer execute(List<String> argv);
}
