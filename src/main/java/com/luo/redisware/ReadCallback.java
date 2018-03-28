package com.luo.redisware;

import redis.clients.jedis.ShardedJedis;

/**
 * 读失败回调类
 *
 * @author xiangnan
 * @date 2018/3/28 17:32
 */
public interface ReadCallback <T> {
    T callback(ShardedJedis jedis);
}
