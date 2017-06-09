namespace java com.code.labs.shardkv

service KVProxy {

  string get(1: string key);

  bool put(1: string key, 2: string value);

}