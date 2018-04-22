package com.test.redisware;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author xiangnan
 * date 2018/3/19 19:41
 */
public class RedisTest {

    @Test
    public void helloRedis() {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(2);
        poolConfig.setMaxWaitMillis(3000);
        poolConfig.setBlockWhenExhausted(false);

        JedisPool jedisPool = new JedisPool(poolConfig, "192.168.2.33", 6379, 2000, "123456");

        Jedis jedis = jedisPool.getResource();

        System.out.println(jedis.get("name"));

        jedis.close();
    }

    @Test
    public void aa() {
        for (int i = -5; i <= 4; i++) {
            System.out.println(i % 5);
        }
    }

}
