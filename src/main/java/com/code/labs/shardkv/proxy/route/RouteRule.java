package com.code.labs.shardkv.proxy.route;

public abstract class RouteRule {

  protected int shardSize;

  public RouteRule(int shardSize) {
    this.shardSize = shardSize;
  }

  public abstract int route(String key);

}
