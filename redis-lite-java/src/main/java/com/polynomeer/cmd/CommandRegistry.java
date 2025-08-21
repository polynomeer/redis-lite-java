package com.polynomeer.cmd;

import com.polynomeer.db.Db;
import com.polynomeer.net.ClientConn;
import com.polynomeer.pubsub.PubSubBroker;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple command registry/dispatcher with context.
 */
public final class CommandRegistry {
    private static final Map<String, Command> CMDS = new HashMap<>();

    private CommandRegistry() {
    }

    public static void initDefaults(Db db, PubSubBroker broker) {
        CMDS.clear();
        PingEchoCommands.register(CMDS);
        StringCommands.register(CMDS, db);
        HashCommands.register(CMDS, db);
        PubSubCommands.register(CMDS, broker);
    }

    public static void register(String name, Command c) {
        CMDS.put(name.toUpperCase(), c);
    }

    public static ByteBuffer dispatch(List<String> argv, ClientConn ctx) {
        if (argv.isEmpty()) {
            return RespWriter.error("ERR empty command");
        }
        String name = argv.get(0).toUpperCase();
        Command c = CMDS.get(name);
        if (c == null) {
            return RespWriter.error("ERR unknown command '" + argv.get(0) + "'");
        }
        return c.execute(argv, ctx);
    }
}
