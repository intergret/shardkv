namespace java com.code.labs.shardkv

struct GetResponse {
  1: required bool success,
  2: optional string value,
  3: optional string msg
}

struct PutResponse {
  1: required bool success,
  2: optional string msg
}

service KVServer {

  GetResponse get(1: string key);

  PutResponse put(1: string key, 2: string value);

}