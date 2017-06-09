package com.code.labs.shardkv.proxy.route;

import java.util.SortedMap;
import java.util.TreeMap;

import com.code.labs.shardkv.common.hash.MurmurHash;

public class ConsistentHashRoute extends RouteRule {

  private TreeMap<Long,Integer> shardNodes = new TreeMap<>();

  ConsistentHashRoute(int shardSize) {
    super(shardSize);

    shardNodes.clear();
    for (int shard = 0; shard < shardSize; ++shard) {
      for (int n = 0; n < 100; n++) {
        shardNodes.put(MurmurHash.hash("SHARD-" + shard + "-NODE-" + n), shard);
      }
    }
  }

  @Override
  int route(String key) {
    SortedMap<Long,Integer> tail = shardNodes.tailMap(MurmurHash.hash(key));
    if (tail.isEmpty()) {
      return shardNodes.get(shardNodes.firstKey());
    }
    return tail.get(tail.firstKey());
  }
}
