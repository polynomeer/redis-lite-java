package com.polynomeer.pubsub;


import com.polynomeer.net.ClientConn;
import com.polynomeer.resp.RespWriter;

import java.util.*;

/**
 * In-memory Pub/Sub broker:
 * - channel -> set of connections
 * - subscribe/unsubscribe from reactor thread
 * - non-blocking broadcast: push to each subscriber's write queue
 */
public final class PubSubBroker {

    private final Map<String, Set<ClientConn>> channels = new HashMap<>();

    /**
     * Subscribe ctx to channel; returns new subscription count for ctx.
     */
    public int subscribe(String channel, ClientConn ctx) {
        Set<ClientConn> set = channels.computeIfAbsent(channel, k -> new HashSet<>());
        set.add(ctx);
        ctx.addSubscription(channel);
        return ctx.subscriptionCount();
    }

    /**
     * Unsubscribe ctx from channel; returns new subscription count for ctx.
     */
    public int unsubscribe(String channel, ClientConn ctx) {
        Set<ClientConn> set = channels.get(channel);
        if (set != null) {
            set.remove(ctx);
            if (set.isEmpty()) channels.remove(channel);
        }
        ctx.removeSubscription(channel);
        return ctx.subscriptionCount();
    }

    /**
     * Unsubscribe ctx from all channels.
     * If 'known' is null, this method will not iterate it; it will push acks by snapshotting.
     */
    public void unsubscribeAll(ClientConn ctx, Set<String> known) {
        // Snapshot to avoid concurrent modification (still single-threaded, but safe)
        Set<String> subs = (known != null) ? new HashSet<>(known) : snapshotSubscriptions(ctx);
        for (String ch : subs) {
            int count = unsubscribe(ch, ctx);
            ctx.push(RespWriter.array3("unsubscribe", ch, count));
        }
    }

    /**
     * Publish payload to channel; returns number of subscribers delivered to.
     */
    public int publish(String channel, String payload) {
        Set<ClientConn> set = channels.get(channel);
        if (set == null || set.isEmpty()) return 0;
        int n = 0;
        // Snapshot to avoid surprises if any client unsubscribes while iterating
        List<ClientConn> targets = new ArrayList<>(set);
        for (ClientConn c : targets) {
            c.push(RespWriter.arrayMessage(channel, payload));
            n++;
        }
        return n;
    }

    private Set<String> snapshotSubscriptions(ClientConn ctx) {
        // We don't keep a reverse map; ask ctx to enumerate.
        // For simplicity, we can't access ctx's set directly; handled through known=null path
        // and per-channel removal above after snapshotting from channels map.
        // Create a snapshot by scanning channels. O(#channels), acceptable for this step.
        Set<String> subs = new HashSet<>();
        for (Map.Entry<String, Set<ClientConn>> e : channels.entrySet()) {
            if (e.getValue().contains(ctx)) subs.add(e.getKey());
        }
        return subs;
    }
}
