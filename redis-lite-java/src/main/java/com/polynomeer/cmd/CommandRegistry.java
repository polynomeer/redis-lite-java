package com.polynomeer.cmd;

import com.polynomeer.db.Db;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Simple command registry/dispatcher.
 * Registers core commands and dispatches argv lists to handlers.
 */
public final class CommandRegistry {
    private static final Map<String, Command> CMDS = new HashMap<>();

    private CommandRegistry() {
    }

    public static void initDefaults(Db db) {
        CMDS.clear();
        PingEchoCommands.register(CMDS);
        StringCommands.register(CMDS, db);
        // add more modules later (hash, pubsub, etc.)
    }

    public static void register(String name, Command c) {
        CMDS.put(name.toUpperCase(), c);
    }

    public static ByteBuffer dispatch(List<String> argv) {
        if (argv.isEmpty()) {
            return RespWriter.error("ERR empty command");
        }
        String name = argv.get(0).toUpperCase();
        Command c = CMDS.get(name);
        if (c == null) {
            return RespWriter.error("ERR unknown command '" + argv.get(0) + "'");
        }
        return c.execute(argv);
    }
}

