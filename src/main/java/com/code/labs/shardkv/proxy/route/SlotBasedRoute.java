package com.code.labs.shardkv.proxy.route;

import java.util.TreeMap;

public class SlotBasedRoute extends RouteRule {

  private int SLOT_NUMBER = 1024;

  private TreeMap<Integer,Integer> shardSlots = new TreeMap<>();

  SlotBasedRoute(int shardSize) {
    super(shardSize);

    shardSlots.clear();
    int slotSizePerShard = SLOT_NUMBER / shardSize;
    for (int slot = 0; slot < SLOT_NUMBER; slot++) {
      shardSlots.put(slot, slot / slotSizePerShard);
    }
  }

  @Override
  int route(String key) {
    int slot = Math.abs(key.hashCode() % SLOT_NUMBER);
    return shardSlots.get(slot);
  }
}
