package com.code.labs.shardkv.proxy.route;

import java.util.concurrent.ThreadLocalRandom;

public class RandomRoute extends RouteRule {

  RandomRoute(int shardSize) {
    super(shardSize);
  }

  @Override
  int route(String key) {
    return ThreadLocalRandom.current().nextInt(shardSize);
  }
}
