package com.code.labs.shardkv;

import com.code.labs.shardkv.client.ShardKVClient;
import com.code.labs.shardkv.common.Env;

public class TestShardKvClient {

  public static void main(String[] args) throws Exception {
    ShardKVClient client = new ShardKVClient(Env.DEBUG);
    System.out.println(client.put("k1", "v1"));
    System.out.println(client.get("k1"));
    client.close();
    System.exit(0);
  }
}
