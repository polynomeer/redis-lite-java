package com.polynomeer.cmd;

import com.polynomeer.db.Db;
import com.polynomeer.lua.LuaEngine;
import com.polynomeer.net.ClientConn;
import com.polynomeer.pubsub.PubSubBroker;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CommandRegistry {
    private static final Map<String, Command> CMDS = new HashMap<>();

    private CommandRegistry() {
    }

    public static void initDefaults(Db db, PubSubBroker broker, LuaEngine lua) {
        CMDS.clear();
        PingEchoCommands.register(CMDS);
        StringCommands.register(CMDS, db);      // GET/SET/DEL/SETNX
        HashCommands.register(CMDS, db);        // H*
        ExpireCommands.register(CMDS, db);      // PEXPIRE/PTTL
        PubSubCommands.register(CMDS, broker);  // SUB/UNSUB/PUBLISH
        TxCommands.register(CMDS);               // MULTI/EXEC/DISCARD
        LuaCommands.register(CMDS, lua);        // EVAL/EVALSHA/SCRIPT
    }

    public static void register(String name, Command c) {
        CMDS.put(name.toUpperCase(), c);
    }

    /**
     * Normal dispatch path used by ClientConn. Handles transactional queuing.
     */
    public static ByteBuffer dispatch(List<String> argv, ClientConn ctx) {
        if (argv.isEmpty()) return RespWriter.error("ERR empty command");
        String name = argv.get(0).toUpperCase();

        Command c = CMDS.get(name);
        if (c == null) {
            if (ctx.isInTxn()) {
                // Unknown command during MULTI: mark dirty and return immediate error
                ctx.markTxnDirty();
                return RespWriter.error("ERR unknown command '" + argv.get(0) + "'");
            }
            return RespWriter.error("ERR unknown command '" + argv.get(0) + "'");
        }

        // MULTI/EXEC/DISCARD are handled always (even inside MULTI)
        if (TxCommands.isTxControl(name)) {
            return c.execute(argv, ctx);
        }

        // If in transaction and not EXEC/DISCARD/MULTI: queue instead of executing
        if (ctx.isInTxn() && !ctx.isBypassTxn()) {
            ctx.queueTxn(argv);
            return RespWriter.simpleString("QUEUED");
        }

        // Normal immediate execution
        return c.execute(argv, ctx);
    }

    /**
     * Internal: execute a single command immediately, bypassing transaction queueing.
     */
    static ByteBuffer dispatchImmediate(List<String> argv, ClientConn ctx) {
        try {
            ctx.setBypassTxn(true);
            return dispatch(argv, ctx);
        } finally {
            ctx.setBypassTxn(false);
        }
    }

    /**
     * Internal: EXEC implementation. Runs queued commands and returns an array of their replies.
     */
    static ByteBuffer execQueued(ClientConn ctx) {
        if (!ctx.isInTxn()) return RespWriter.error("ERR EXEC without MULTI");
        if (ctx.isTxnDirty()) {
            ctx.endTxn();
            return RespWriter.error("EXECABORT Transaction discarded because of previous errors.");
        }
        List<List<String>> queued = ctx.drainTxnQueue();
        List<ByteBuffer> replies = new ArrayList<>(queued.size());
        for (List<String> a : queued) {
            ByteBuffer r = dispatchImmediate(a, ctx);
            if (r == null) {
                // Some commands (e.g., SUBSCRIBE) normally return null because they push.
                // In transaction context, such commands are unusual; encode as $-1 for simplicity.
                r = RespWriter.nullBulk();
            }
            replies.add(r);
        }
        ctx.endTxn();
        return RespWriter.arrayOfFrames(replies);
    }
}
