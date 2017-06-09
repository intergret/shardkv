package com.code.labs.shardkv.proxy.route;

public class DefaultRoute extends RouteRule {

  public DefaultRoute(int shardSize) {
    super(shardSize);
  }

  @Override
  public int route(String key) {
    return Math.abs(key.hashCode() % shardSize);
  }
}
