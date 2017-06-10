package com.code.labs.shardkv.common;

public class Config {

  public static final int SHARD_SIZE = 3;

  public static final String ZK = "127.0.0.1:2181";

  public static final String ZK_SERVER_PATH = "/shardkv/server/shards/%s/%s";

  public static final String ZK_PROXY_PATH = "/shardkv/proxy";

}
