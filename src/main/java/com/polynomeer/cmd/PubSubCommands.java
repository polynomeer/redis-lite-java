package com.polynomeer.cmd;


import com.polynomeer.net.ClientConn;
import com.polynomeer.pubsub.PubSubBroker;
import com.polynomeer.resp.RespWriter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Pub/Sub commands:
 * - SUBSCRIBE channel [channel ...]         -> ["subscribe", ch, count] per channel
 * - UNSUBSCRIBE [channel ...]               -> ["unsubscribe", ch, count] per channel (or all if none)
 * - PUBLISH channel message                 -> :<receivers>
 * <p>
 * Notes:
 * - This step implements channel-based Pub/Sub (no PSUBSCRIBE/patterns).
 * - A connection in subscribed mode normally accepts only pubsub cmds;
 * for simplicity we don't enforce strict mode here.
 */
public final class PubSubCommands {
    private PubSubCommands() {
    }

    public static void register(Map<String, Command> reg, PubSubBroker broker) {
        reg.put("SUBSCRIBE", (argv, ctx) -> subscribe(broker, argv, ctx));
        reg.put("UNSUBSCRIBE", (argv, ctx) -> unsubscribe(broker, argv, ctx));
        reg.put("PUBLISH", (argv, ctx) -> publish(broker, argv));
    }

    private static ByteBuffer subscribe(PubSubBroker broker, List<String> argv, ClientConn ctx) {
        if (argv.size() < 2) {
            return RespWriter.error("ERR wrong number of arguments for 'SUBSCRIBE'");
        }
        // For every channel: add to broker and push ack ["subscribe", ch, count]
        for (int i = 1; i < argv.size(); i++) {
            String ch = argv.get(i);
            int count = broker.subscribe(ch, ctx);
            ctx.push(RespWriter.array3("subscribe", ch, count));
        }
        // All acks already pushed
        return null;
    }

    private static ByteBuffer unsubscribe(PubSubBroker broker, List<String> argv, ClientConn ctx) {
        // No arg: unsubscribe from all channels
        if (argv.size() == 1) {
            broker.unsubscribeAll(ctx, null); // broker will pull set from ctx
            // broker will push ["unsubscribe", ch, count] for each
            return null;
        }
        for (int i = 1; i < argv.size(); i++) {
            String ch = argv.get(i);
            int count = broker.unsubscribe(ch, ctx);
            ctx.push(RespWriter.array3("unsubscribe", ch, count));
        }
        return null;
    }

    private static ByteBuffer publish(PubSubBroker broker, List<String> argv) {
        if (argv.size() != 3) {
            return RespWriter.error("ERR wrong number of arguments for 'PUBLISH'");
        }
        String ch = argv.get(1);
        String payload = argv.get(2);
        int delivered = broker.publish(ch, payload);
        return RespWriter.integer(delivered);
    }
}
