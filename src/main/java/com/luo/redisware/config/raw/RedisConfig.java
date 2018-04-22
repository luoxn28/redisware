package com.luo.redisware.config.raw;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import redis.clients.jedis.JedisShardInfo;

/**
 * Redis实例配置，包括 master slave
 *
 * @author xiangnan
 * date 2018/3/20 14:20
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RedisConfig {
    private String sentinelName;
    private JedisShardInfo master;
    private JedisShardInfo slave;
}
