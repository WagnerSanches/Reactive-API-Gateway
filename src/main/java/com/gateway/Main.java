package com.gateway;

public class Main {
    public static void main(String[] args) throws Exception {
        new GatewayServer(8080).start();
    }
}