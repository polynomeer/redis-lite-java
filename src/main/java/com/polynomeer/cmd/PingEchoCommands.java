package com.polynomeer.cmd;

import com.polynomeer.net.ClientConn;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class PingEchoCommands {

    public static void register(Map<String, Command> reg) {
        reg.put("PING", PingEchoCommands::ping);
        reg.put("ECHO", PingEchoCommands::echo);
    }

    private static ByteBuffer ping(List<String> argv, ClientConn ctx) {
        if (argv.size() == 1) return RespWriter.simpleString("PONG");
        if (argv.size() == 2) return RespWriter.simpleString(argv.get(1));
        return RespWriter.error("ERR wrong number of arguments for 'PING'");
    }

    private static ByteBuffer echo(List<String> argv, ClientConn ctx) {
        if (argv.size() != 2) {
            return RespWriter.error("ERR wrong number of arguments for 'ECHO'");
        }
        return RespWriter.bulkString(argv.get(1));
    }

    // Legacy compatibility dispatcher removed (registry path is used)
}
