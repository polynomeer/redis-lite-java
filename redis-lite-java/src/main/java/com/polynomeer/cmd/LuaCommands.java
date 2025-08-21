package com.polynomeer.cmd;

import com.polynomeer.lua.LuaEngine;
import com.polynomeer.lua.LuaRespEncoder;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Lua scripting commands:
 * - EVAL script numkeys key [key ...] arg [arg ...]
 * - EVALSHA sha1 numkeys key [key ...] arg [arg ...]
 * - SCRIPT LOAD script
 * - SCRIPT EXISTS sha1 [sha1 ...]
 * - SCRIPT FLUSH
 * (SCRIPT KILL is not supported in this step)
 */
public final class LuaCommands {
    private LuaCommands() {
    }

    public static void register(Map<String, Command> reg, LuaEngine lua) {
        reg.put("EVAL", (argv, ctx) -> eval(lua, argv));
        reg.put("EVALSHA", (argv, ctx) -> evalsha(lua, argv));
        reg.put("SCRIPT", (argv, ctx) -> script(lua, argv));
    }

    private static ByteBuffer eval(LuaEngine lua, List<String> argv) {
        if (argv.size() < 3) return RespWriter.error("ERR wrong number of arguments for 'EVAL'");
        String script = argv.get(1);
        int numkeys;
        try {
            numkeys = Integer.parseInt(argv.get(2));
        } catch (Exception e) {
            return RespWriter.error("ERR value is not an integer or out of range");
        }

        if (argv.size() < 3 + numkeys) return RespWriter.error("ERR wrong number of arguments for 'EVAL'");

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numkeys; i++) keys.add(argv.get(3 + i));
        List<String> args = new ArrayList<>();
        for (int i = 3 + numkeys; i < argv.size(); i++) args.add(argv.get(i));

        try {
            var luaVal = lua.eval(script, keys, args);
            return LuaRespEncoder.encode(luaVal);
        } catch (RuntimeException e) {
            return RespWriter.error("ERR " + e.getMessage());
        }
    }

    private static ByteBuffer evalsha(LuaEngine lua, List<String> argv) {
        if (argv.size() < 3) return RespWriter.error("ERR wrong number of arguments for 'EVALSHA'");
        String sha = argv.get(1);
        int numkeys;
        try {
            numkeys = Integer.parseInt(argv.get(2));
        } catch (Exception e) {
            return RespWriter.error("ERR value is not an integer or out of range");
        }

        if (argv.size() < 3 + numkeys) return RespWriter.error("ERR wrong number of arguments for 'EVALSHA'");

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numkeys; i++) keys.add(argv.get(3 + i));
        List<String> args = new ArrayList<>();
        for (int i = 3 + numkeys; i < argv.size(); i++) args.add(argv.get(i));

        try {
            var luaVal = lua.evalSha(sha, keys, args);
            return LuaRespEncoder.encode(luaVal);
        } catch (LuaEngine.NoScript e) {
            return RespWriter.error("NOSCRIPT No matching script. Please use EVAL.");
        } catch (RuntimeException e) {
            return RespWriter.error("ERR " + e.getMessage());
        }
    }

    private static ByteBuffer script(LuaEngine lua, List<String> argv) {
        if (argv.size() < 2) return RespWriter.error("ERR wrong number of arguments for 'SCRIPT'");
        String sub = argv.get(1).toUpperCase();
        switch (sub) {
            case "LOAD":
                if (argv.size() != 3) return RespWriter.error("ERR wrong number of arguments for 'SCRIPT LOAD'");
                String sha = lua.scriptLoad(argv.get(2));
                return RespWriter.bulkString(sha);
            case "EXISTS":
                if (argv.size() < 3) return RespWriter.error("ERR wrong number of arguments for 'SCRIPT EXISTS'");
                int[] arr = new int[argv.size() - 2];
                for (int i = 2; i < argv.size(); i++) arr[i - 2] = lua.scriptExists(argv.get(i)) ? 1 : 0;
                return RespWriter.arrayOfIntegers(arr);
            case "FLUSH":
                lua.scriptFlush();
                return RespWriter.simpleString("OK");
            case "KILL":
                return RespWriter.error("ERR SCRIPT KILL not supported");
            default:
                return RespWriter.error("ERR unknown subcommand or wrong number of arguments for 'SCRIPT'");
        }
    }
}
