package com.code.labs.shardkv.proxy.route;

public abstract class RouteRule {

  protected int shardSize;

  RouteRule(int shardSize) {
    this.shardSize = shardSize;
  }

  abstract int route(String key);

}
