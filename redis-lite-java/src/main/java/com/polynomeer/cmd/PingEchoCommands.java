package com.polynomeer.cmd;

import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Minimal dispatcher for PING/ECHO used in early bring-up:
 * - PING            -> +PONG
 * - PING <msg>      -> +<msg>  (redis-cli echoes custom text)
 * - ECHO <msg>      -> $<len>\r\n<msg>\r\n
 */
public class PingEchoCommands {

    public static ByteBuffer dispatch(List<String> argv) {
        if (argv.isEmpty()) {
            return RespWriter.error("ERR empty command");
        }
        String cmd = argv.get(0).toUpperCase();

        switch (cmd) {
            case "PING":
                if (argv.size() == 1) return RespWriter.simpleString("PONG");
                if (argv.size() == 2) return RespWriter.simpleString(argv.get(1));
                return RespWriter.error("ERR wrong number of arguments for 'PING'");
            case "ECHO":
                if (argv.size() != 2) {
                    return RespWriter.error("ERR wrong number of arguments for 'ECHO'");
                }
                return RespWriter.bulkString(argv.get(1));
            default:
                return RespWriter.error("ERR unknown command '" + argv.get(0) + "'");
        }
    }
}
