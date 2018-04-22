package com.luo.redisware.callback;

import redis.clients.jedis.ShardedJedis;

/**
 * redis故障转移（主备切换）回调类
 *
 * @author xiangnan
 * date 2018/3/28 17:01
 */
public interface SentinelCallback <T> {
    T callback(ShardedJedis jedis);
}
