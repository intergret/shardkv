package com.code.labs.shardkv.server.storage;

public interface KVStore {

  String get(String key);

  boolean put(String key, String value);

}
