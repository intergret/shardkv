namespace java com.code.labs.shardkv

service KVServer {

  string get(1: string key);

  bool put(1: string key, 2: string value);

}