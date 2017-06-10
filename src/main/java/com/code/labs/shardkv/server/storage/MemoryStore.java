package com.code.labs.shardkv.server.storage;

import java.util.concurrent.ConcurrentHashMap;

public class MemoryStore implements KVStore {

  private ConcurrentHashMap<String,String> memoryStore = new ConcurrentHashMap<>();

  @Override
  public String get(String key) {
    return memoryStore.get(key);
  }

  @Override
  public boolean put(String key, String value) {
    memoryStore.put(key, value);
    return true;
  }
}
