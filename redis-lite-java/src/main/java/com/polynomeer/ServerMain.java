package com.polynomeer;

import com.polynomeer.net.Reactor;

public class ServerMain {
    public static void main(String[] args) throws Exception {
        int port = 6379;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        Reactor reactor = new Reactor(port);
        reactor.start(); // blocking loop
    }
}
