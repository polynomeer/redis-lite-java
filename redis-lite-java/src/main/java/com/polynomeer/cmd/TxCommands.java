package com.polynomeer.cmd;

import com.polynomeer.net.ClientConn;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * MULTI/EXEC/DISCARD transaction control.
 * - MULTI: begin queuing; subsequent commands return +QUEUED
 * - EXEC:  execute queued commands atomically; return RESP array of replies
 * - DISCARD: clear queue and exit transactional state
 */
public final class TxCommands {
    private TxCommands() {
    }

    public static void register(Map<String, Command> reg) {
        reg.put("MULTI", TxCommands::multi);
        reg.put("EXEC", TxCommands::exec);
        reg.put("DISCARD", TxCommands::discard);
    }

    public static boolean isTxControl(String name) {
        switch (name) {
            case "MULTI":
            case "EXEC":
            case "DISCARD":
                return true;
            default:
                return false;
        }
    }

    private static ByteBuffer multi(List<String> argv, ClientConn ctx) {
        if (argv.size() != 1) return RespWriter.error("ERR wrong number of arguments for 'MULTI'");
        if (ctx.isInTxn()) return RespWriter.error("ERR MULTI calls can not be nested");
        ctx.beginTxn();
        return RespWriter.simpleString("OK");
    }

    private static ByteBuffer exec(List<String> argv, ClientConn ctx) {
        if (argv.size() != 1) return RespWriter.error("ERR wrong number of arguments for 'EXEC'");
        return CommandRegistry.execQueued(ctx);
    }

    private static ByteBuffer discard(List<String> argv, ClientConn ctx) {
        if (argv.size() != 1) return RespWriter.error("ERR wrong number of arguments for 'DISCARD'");
        if (!ctx.isInTxn()) return RespWriter.error("ERR DISCARD without MULTI");
        ctx.endTxn();
        return RespWriter.simpleString("OK");
    }
}
