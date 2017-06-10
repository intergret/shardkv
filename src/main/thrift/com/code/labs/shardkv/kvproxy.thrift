namespace java com.code.labs.shardkv

struct ProxyGetResponse {
  1: required i32 shardId
  2: required bool success,
  3: optional string value,
  4: optional string msg
}

struct ProxyPutResponse {
  1: required i32 shardId
  2: required bool success,
  3: optional string msg
}

service KVProxy {

  ProxyGetResponse get(1: string key);

  ProxyPutResponse put(1: string key, 2: string value);

}