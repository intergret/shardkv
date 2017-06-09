package com.code.labs.shardkv.proxy.route;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

public class RoundRobinRoute extends RouteRule {

  private Iterator<Integer> shardIter;

  public RoundRobinRoute(int shardSize) {
    super(shardSize);

    List<Integer> shardList = new ArrayList<>();
    for (int i = 0; i < shardSize; i++) {
      shardList.add(i);
    }
    shardIter = Iterators.cycle(shardList);
  }

  @Override
  public int route(String key) {
    return shardIter.next();
  }
}
