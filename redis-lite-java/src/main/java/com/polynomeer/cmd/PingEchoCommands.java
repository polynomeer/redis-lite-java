package com.polynomeer.cmd;

import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Minimal PING/ECHO commands:
 * - PING            -> +PONG
 * - PING <msg>      -> +<msg>
 * - ECHO <msg>      -> $<len>\r\n<msg>\r\n
 */
public class PingEchoCommands {

    public static void register(Map<String, Command> reg) {
        reg.put("PING", PingEchoCommands::ping);
        reg.put("ECHO", PingEchoCommands::echo);
    }

    private static ByteBuffer ping(List<String> argv) {
        if (argv.size() == 1) return RespWriter.simpleString("PONG");
        if (argv.size() == 2) return RespWriter.simpleString(argv.get(1));
        return RespWriter.error("ERR wrong number of arguments for 'PING'");
    }

    private static ByteBuffer echo(List<String> argv) {
        if (argv.size() != 2) {
            return RespWriter.error("ERR wrong number of arguments for 'ECHO'");
        }
        return RespWriter.bulkString(argv.get(1));
    }

    /**
     * Legacy direct dispatcher (kept for compatibility if needed).
     */
    public static ByteBuffer dispatch(List<String> argv) {
        String cmd = argv.get(0).toUpperCase();
        switch (cmd) {
            case "PING":
                return ping(argv);
            case "ECHO":
                return echo(argv);
            default:
                return RespWriter.error("ERR unknown command '" + argv.get(0) + "'");
        }
    }
}

