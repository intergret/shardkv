package com.code.labs.shardkv.proxy.route;

public class MurmurHashRoute extends RouteRule {

  MurmurHashRoute(int shardSize) {
    super(shardSize);
  }

  @Override
  int route(String key) {
    return Math.abs(key.hashCode() % shardSize);
  }
}
