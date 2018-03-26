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
            jedis = getJedisWrap(key, true);
            return jedis.getJedis().get(key);
        } catch (Exception e) {
            logger.error("get {} error, e={}", key, e);

            Jedis tmpJedis = null;
            try {
                tmpJedis = readFailover(e, jedis);
                return tmpJedis.get(key);
            } catch (Exception e2) {
                logger.error("failover get {} error, e={}", key, e2);
                throw e2;
            } finally {
                if (tmpJedis != null) {
                    tmpJedis.close();
                }
            }

        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public String set(String key, String value) throws Exception {
        Jedis jedis = null;
        try {
            jedis = getJedis(key);
            return jedis.set(key, value);
        } catch (Exception e) {
            logger.warn("set {}={} error, e={}", key, value, e);
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 读失败转移
     */
    private Jedis readFailover(Exception e, JedisWrap jedis) throws Exception {
        if ((jedis == null) || !(e instanceof JedisConnectionException)) {
            throw e;
        }

        int index = jedis.getIndex();
        if (jedis.isMaster()) {
            List<JedisPool> slaveList = slavePoolList.get(index);
            if (CollectionUtil.isEmpty(slaveList)) {
                throw e;
            }

            return slaveList.get(new Random().nextInt(slaveList.size())).getResource();
        } else {
            return masterPoolList.get(index).getResource();
        }
    }

    private Jedis getJedis(String key) throws JedisException, UnsupportedEncodingException {
        int index = (int) (this.algo.hash(key.getBytes("UTF-8")) % masterPoolList.size());
        if (index < 0) {
            index += masterPoolList.size();
        }

        return masterPoolList.get(index).getResource();
    }

    private JedisWrap getJedisWrap(String key, boolean isRead) throws JedisException, UnsupportedEncodingException {
        int index = (int) (this.algo.hash(key.getBytes("UTF-8")) % masterPoolList.size());
        index = index < 0 ? index + masterPoolList.size() : index;

        if (!isRead) {
            return new JedisWrap(masterPoolList.get(index).getResource(), index, true);
        } else {
            List<JedisPool> poolList = slavePoolList.get(index);
            if (poolList.isEmpty()) {
                return new JedisWrap(masterPoolList.get(index).getResource(), index, true);
            } else {
                // master + slave 随机读
                int n = new Random().nextInt(poolList.size() + 1);
                if (n == poolList.size()) {
                    return new JedisWrap(masterPoolList.get(index).getResource(), index, true);
                } else {
                    return new JedisWrap(poolList.get(n).getResource(), index, false);
                }
            }
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
