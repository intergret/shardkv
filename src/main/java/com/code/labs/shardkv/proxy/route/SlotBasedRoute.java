package com.code.labs.shardkv.proxy.route;

import java.util.TreeMap;

public class SlotBasedRoute extends RouteRule {

  private int SLOT_NUMBER = 1024;

  private TreeMap<Integer,Integer> shardSlots = new TreeMap<>();

  public SlotBasedRoute(int shardSize) {
    super(shardSize);

    shardSlots.clear();
    int slotSizePerShard = SLOT_NUMBER / shardSize;
    int shardWithMoreSlot = SLOT_NUMBER - slotSizePerShard * shardSize;

    int slot = 0;
    int firstPartSlots = shardWithMoreSlot * (slotSizePerShard + 1);
    for (; slot < firstPartSlots; slot++) {
      shardSlots.put(slot, slot / (slotSizePerShard + 1));
    }
    for (; slot < SLOT_NUMBER; slot++) {
      shardSlots.put(slot, (slot - firstPartSlots) / slotSizePerShard + shardWithMoreSlot);
    }
  }

  @Override
  public int route(String key) {
    int slot = Math.abs(key.hashCode() % SLOT_NUMBER);
    return shardSlots.get(slot);
  }
}
