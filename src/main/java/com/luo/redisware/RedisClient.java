package com.luo.redisware;

import cn.hutool.core.collection.CollectionUtil;
import com.luo.redisware.config.raw.RedisConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Hashing;
import redis.clients.util.MurmurHash;

import java.io.UnsupportedEncodingException;
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

    private List<RedisConfig> redisList;

    private List<JedisPool> masterPoolList = new ArrayList<>();
    private List<List<JedisPool>> slavePoolList = new ArrayList<>();

    private Hashing algo = new MurmurHash();

    public RedisClient(List<RedisConfig> redisList) {
        if (CollectionUtil.isEmpty(redisList)) {
            throw new RuntimeException("List<RedisConfig> redisList is empty");
        }

        this.redisList = redisList;
        for (RedisConfig config : redisList) {
            this.masterPoolList.add(RedisConfig.buildPool(config.getPoolConfig(), config.getMaster()));
            this.slavePoolList.add(RedisConfig.buildPoolList(config.getPoolConfig(), config.getSlaveList()));
        }
    }

    public String get(String key) throws Exception {
        JedisWrap jedis = null;
        try {
            jedis = getJedis(key);
            return jedis.getJedis().get(key);
        } catch (Exception e) {
            logger.warn("get {} error, e={}", key, e);

            return readFailover(e, jedis).getJedis().get(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

    }

    /**
     * 读失败转移
     */
    private JedisWrap readFailover(Exception e, JedisWrap jedis) throws Exception {
        if ((jedis == null) || !(e instanceof JedisConnectionException)) {
            throw e;
        }

        if (jedis.isMaster()) {
            if (CollectionUtil.isEmpty(slavePoolList.get(jedis.getIndex()))) {
                throw e;
            }

            return getSlaveJedis(jedis.getIndex());
        } else {
            return new JedisWrap(masterPoolList.get(jedis.getIndex()).getResource(), jedis.getIndex(), true);
        }
    }

    public String set(String key, String value) throws Exception {
        JedisWrap jedis = null;
        try {
            jedis = getJedis(key);
            return jedis.getJedis().set(key, value);
        } catch (Exception e) {
            logger.warn("set {} error, e={}", key, e);
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private JedisWrap getSlaveJedis(int index) {
        try {
            List<JedisPool> poolList = slavePoolList.get(index);
            if (CollectionUtil.isEmpty(poolList)) {
                throw new RuntimeException("slave list empty, index=" + index);
            }

            return new JedisWrap(poolList.get(new Random().nextInt(poolList.size())).getResource(),
                    index, false);
        } catch (Exception e) {
            throw new JedisException(e);
        }
    }

    private JedisWrap getJedis(String key) throws JedisException {
        try {
            int index = (int) (this.algo.hash(key.getBytes("UTF-8")) % masterPoolList.size());
            index = index < 0 ? index + masterPoolList.size() : index;
            return new JedisWrap(masterPoolList.get(index).getResource(), index, true);
        } catch (UnsupportedEncodingException e) {
            throw new JedisException(e);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class JedisWrap {
        Jedis jedis;
        int index;
        boolean master;

        void close() {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

}
