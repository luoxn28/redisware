package com.luo.redisware;

import cn.hutool.core.collection.CollectionUtil;
import com.luo.redisware.config.raw.RedisConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 分布式Redis客户端
 *
 * @author xiangnan
 * @date 2018/3/19 11:47
 */
public class RedisClient {
    private static Logger logger = LogManager.getLogger(RedisClient.class);

    private GenericObjectPoolConfig defaultPoolConfig = new GenericObjectPoolConfig();
    {
        // 初始化默认pool配置
        defaultPoolConfig.setMaxIdle(20);
        defaultPoolConfig.setMinIdle(5);
        defaultPoolConfig.setMaxTotal(200);
        defaultPoolConfig.setMaxWaitMillis(2000);
        defaultPoolConfig.setBlockWhenExhausted(false);
    }

    private List<RedisConfig> redisList;

    private ShardedJedisPool masterJedisPool;
    private ShardedJedisPool slaveJedisPool;

    public RedisClient(List<RedisConfig> redisList, GenericObjectPoolConfig poolConfig) {
        if (CollectionUtil.isEmpty(redisList)) {
            throw new RuntimeException("List<RedisConfig> redisList is empty");
        }

        this.redisList = redisList;

        List<JedisShardInfo> masterList = new ArrayList<>();
        List<JedisShardInfo> slaveList = new ArrayList<>();
        for (RedisConfig config : redisList) {
            masterList.add(config.getMaster());
            slaveList.add(config.getSlave() != null ? config.getSlave() : config.getMaster());
        }

        if (poolConfig == null) {
            poolConfig = defaultPoolConfig;
        }
        this.masterJedisPool = new ShardedJedisPool(poolConfig, masterList);
        this.slaveJedisPool = new ShardedJedisPool(poolConfig, slaveList);
    }

    private ShardedJedis getJedis() {
        try {
            return this.masterJedisPool.getResource();
        } catch (Throwable e) {
            logger.error("getJedis error, e={}", e);
            throw e;
        }
    }

    private ShardedJedis getJedis(boolean isRead) {
        try {
            return (isRead && new Random().nextBoolean()) ?
                    this.slaveJedisPool.getResource() : this.masterJedisPool.getResource();
        } catch (Throwable e) {
            logger.error("getJedis error, e={}", e);
            throw e;
        }
    }

    public String set(String key, String value, int expire) throws Exception {
        return set(key, SafeEncoder.encode(value), expire);
    }

    public String set(String key, byte[] value, int expire) throws Exception {
        ShardedJedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.setex(SafeEncoder.encode(key), expire, value);
        } catch (Exception e) {
            logger.warn("set {}={} error, e={}", key, value, e);
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public String get(String key) throws Exception {
        ShardedJedis jedis = null;
        try {
            jedis = getJedis(true);
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("get {} error, e={}", key, e);
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

}
