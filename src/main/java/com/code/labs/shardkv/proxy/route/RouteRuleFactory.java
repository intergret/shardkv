package com.code.labs.shardkv.proxy.route;

import com.code.labs.shardkv.common.Route;

public class RouteRuleFactory {

  public static RouteRule create(Route route, int shardSize) {
    switch (route) {
      case CONSISTENT_HASH:
        return new ConsistentHashRoute(shardSize);
      case RANDOM:
        return new RandomRoute(shardSize);
      case SLOT_BASED:
        return new SlotBasedRoute(shardSize);
      case ROUND_ROBIN:
        return new RoundRobinRoute(shardSize);
      default:
        return new DefaultRoute(shardSize);
    }
  }
}
