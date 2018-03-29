package com.luo.redisware;

import cn.hutool.core.collection.CollectionUtil;
import com.luo.redisware.config.raw.RedisConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

/**
 * 分布式Redis客户端
 *
 * 1. 支持redis master/slave 读写分离
 * 2. 支持redis故障转移，需要配置redis-sentinel
 * 3. 支持master/slave读操作失败转移重试
 *
 * @author xiangnan
 * @date 2018/3/19 11:47
 */
public class RedisClient {
    private static Logger logger = LogManager.getLogger(RedisClient.class);

    public final static int READ_MODE_MASTER_ONLY  = 0;
    public final static int READ_MODE_MASTER_SLAVE = 1;
    public final static int READ_MODE_SLAVE_ONLY   = 2;

    private int readMode = READ_MODE_MASTER_ONLY;

    private GenericObjectPoolConfig defaultPoolConfig = new GenericObjectPoolConfig();
    {
        // 初始化默认pool配置
        defaultPoolConfig.setMaxIdle(20);
        defaultPoolConfig.setMaxTotal(500);
        defaultPoolConfig.setMaxWaitMillis(2000);
        defaultPoolConfig.setBlockWhenExhausted(false);
    }

    private List<RedisConfig> redisList;
    private List<JedisShardInfo> sentinelList = new ArrayList<>();

    private GenericObjectPoolConfig poolConfig = defaultPoolConfig;
    private List<JedisShardInfo> masterList;
    private List<JedisShardInfo> slaveList;

    private ShardedJedisPool masterJedisPool;
    private ShardedJedisPool slaveJedisPool;

    /**
     * 出现读异常时，用于判断当前读操作是master读还是slave读
     */
    private ThreadLocal<Boolean> masterRead = new ThreadLocal<>();

    /**
     * 限制进行故障转移线程数
     */
    private Semaphore sentinelSemaphore = new Semaphore(2);

    public RedisClient(List<RedisConfig> redisList, GenericObjectPoolConfig poolConfig) {
        this(redisList, poolConfig, READ_MODE_MASTER_ONLY, null);
    }

    public RedisClient(List<RedisConfig> redisList, GenericObjectPoolConfig poolConfig, int readMode) {
        this(redisList, poolConfig, readMode, null);
    }

    public RedisClient(List<RedisConfig> redisList, GenericObjectPoolConfig poolConfig, List<JedisShardInfo> sentinelList) {
        this(redisList, poolConfig, READ_MODE_MASTER_ONLY, sentinelList);
    }

    public RedisClient(List<RedisConfig> redisList, GenericObjectPoolConfig poolConfig, int readMode,
                       List<JedisShardInfo> sentinelList) {
        if (CollectionUtil.isEmpty(redisList)) {
            throw new RuntimeException("List<RedisConfig> redisList is empty");
        }

        this.redisList = redisList;
        if ((READ_MODE_MASTER_ONLY <= readMode) && (readMode <= READ_MODE_SLAVE_ONLY)) {
            this.readMode = readMode;
        }

        if (poolConfig != null) {
            this.poolConfig = poolConfig;
        }
        if (!CollectionUtil.isEmpty(sentinelList)) {
            this.sentinelList = sentinelList;

            for (JedisShardInfo sentinelConfig : sentinelList) {
                List<String> monitorList = new ArrayList<>();
                for (RedisConfig redisConfig : redisList) {
                    monitorList.addAll(new Jedis(sentinelConfig.getHost(), sentinelConfig.getPort()).
                            sentinelGetMasterAddrByName(redisConfig.getSentinelName()));
                }

                logger.info("sentinel {}:{} monitorList={}", sentinelConfig.getHost(), sentinelConfig.getPort(),
                        monitorList);
            }
        }

        List<JedisShardInfo> masterList = new ArrayList<>();
        List<JedisShardInfo> slaveList = new ArrayList<>();
        for (RedisConfig config : redisList) {
            masterList.add(config.getMaster());
            slaveList.add(config.getSlave() != null ? config.getSlave() : config.getMaster());
        }

        this.masterList = masterList;
        this.slaveList = slaveList;
        intitJedisPool(this.poolConfig, this.masterList, this.slaveList);
    }

    /**
     * 初始化master、slave连接池
     */
    private void intitJedisPool(GenericObjectPoolConfig poolConfig,
                                List<JedisShardInfo> masterList, List<JedisShardInfo> slaveList) {
        this.masterJedisPool = new ShardedJedisPool(poolConfig, masterList);
        this.slaveJedisPool = new ShardedJedisPool(poolConfig, slaveList);
    }

    /**
     * 获取master jedis连接
     */
    private ShardedJedis getJedis() {
        try {
            return this.masterJedisPool.getResource();
        } catch (Exception e) {
            logger.error("getJedis error, e={}", e);
            throw e;
        }
    }

    /**
     * 获取slave jedis连接
     */
    private ShardedJedis getSlaveJedis() {
        try {
            return this.slaveJedisPool.getResource();
        } catch (Throwable e) {
            logger.error("getJedis error, e={}", e);
            throw e;
        }
    }

    /**
     * 根据readMode获取jedis连接
     */
    private ShardedJedis getJedis(boolean isRead) {
        boolean master = true;

        if (isRead) {
            switch (this.readMode) {
                case READ_MODE_SLAVE_ONLY: {
                    master = false;
                    break;
                }
                case READ_MODE_MASTER_SLAVE: {
                    master = new Random().nextBoolean();
                    break;
                }
                case READ_MODE_MASTER_ONLY:
                default: {
                    break;
                }
            }
        }

        if (master) {
            this.masterRead.set(true);
            return getJedis();
        } else {
            this.masterRead.set(false);
            return getSlaveJedis();
        }
    }

    /**
     * 设置key/value，异常时会尝试进行故障转移
     *
     * @param expire 过期时间，单位 s
     * @throws Exception e
     */
    public String set(String key, String value, int expire) throws Exception {
        return set(key, SafeEncoder.encode(value), expire);
    }

    public String set(final String key, final byte[] value, final int expire) throws Exception {
        ShardedJedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.setex(SafeEncoder.encode(key), expire, value);
        } catch (Exception e) {
            logger.error("set {}={} error, e={}", key, value, e);

            return sentinelFailover(e, new SentinelCallback<String>() {
                @Override
                public String callback(ShardedJedis jedis) {
                    return jedis.setex(SafeEncoder.encode(key), expire, value);
                }
            });
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 获取key，异常时会尝试进行失败转移，读操作进行失败转移时不会进行故障转移，
     * 如果redis-sentinel进行了故障转移，则失败转移后就能够读数据了
     *
     * @throws Exception e
     */
    public String get(final String key) throws Exception {
        ShardedJedis jedis = null;
        try {
            jedis = getJedis(true);
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("get {} error, e={}", key, e);

            return readFailover(new ReadCallback<String>() {
                @Override
                public String callback(ShardedJedis jedis) {
                    return jedis.get(key);
                }
            });
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private <T> T readFailover(ReadCallback<T> callback) {
        ShardedJedis jedis = null;
        try {
            Boolean master = this.masterRead.get();
            jedis = (master != null && master) ? getSlaveJedis() : getJedis();
            return callback.callback(jedis);
        } catch (Exception e) {
            logger.error("readFailover error, e={}", e);
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 非读超时异常时故障转移
     *
     * @return e
     * @throws Exception 非读超时异常
     */
    private <T> T sentinelFailover(Exception e, SentinelCallback<T> callback) throws Exception {

        /**
         * JedisDataException 为对redis数据操作异常，此时尝试进行主备切换
         *
         * JedisConnectionException "Read timed out" - redis响应慢
         *                          "Could not get a resource from the pool" - 连接池资源耗尽
         */
        if (!(e instanceof JedisDataException)) {
            if (!(e instanceof JedisConnectionException) || (e.getMessage() == null) ||
                    e.getMessage().contains("Read timed out") || e.getMessage().contains("Could not get a resource from the pool")) {
                throw e;
            }
        }

        if (this.sentinelSemaphore.tryAcquire()) {
            ShardedJedis jedis = null;
            try {
                jedis = sentinelFailover(e);
                return callback.callback(jedis);
            } catch (Exception ex) {
                logger.error("sentinelFailover error: e={}", ex);
                throw e;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }

                this.sentinelSemaphore.release();
            }
        } else {
            throw e;
        }

    }

    /**
     * 非读超时异常时进行主备连接池切换，并返回master连接，调用该方法时，为了防止过多线程进行主备切换使用信号量进行限制
     *
     * @param e 非读超市异常类
     * @return master连接
     * @throws Exception e
     */
    private ShardedJedis sentinelFailover(Exception e) throws Exception {

        // failover, Redis master实例可能挂了
        List<Jedis> jedisList = new ArrayList<>();
        for (JedisShardInfo redisConfig : this.sentinelList) {
            jedisList.add(new Jedis(redisConfig.getHost(), redisConfig.getPort()));
        }

        boolean sentinelFailover = false;
        for (RedisConfig redisConfig : this.redisList) {
            JedisShardInfo slave = redisConfig.getSlave();

            if (redisConfig.getMaster() == slave) {
                // 只配置了master，无法进行主备切换
                continue;
            }

            for (Jedis jedis : jedisList) {
                try {
                    List<String> masterList = jedis.sentinelGetMasterAddrByName(redisConfig.getSentinelName());
                    if (masterList.contains(String.valueOf(slave.getPort())) &&
                            (masterList.contains(slave.getHost()) || masterList.contains("127.0.0.1"))) {
                        sentinelFailover = true;

                        // sentinel已经进行了主备切换，这里也跟着进行主备信息切换
                        redisConfig.setSlave(redisConfig.getMaster());
                        redisConfig.setMaster(slave);
                        break;
                    }
                } catch (Exception ex) {
                    // redis-sentinel可能挂了，单个redis-sentinel挂了继续使用其他redis-sentinel
                    logger.error("redis-sentinel over... sentinelList={}", this.sentinelList);
                }
            }
        }

        if (!sentinelFailover) {
            logger.info("sentinelFailover is false, redis-sentinel可能还没有进行故障转移");
            throw e;
        }

        logger.info("sentinelFailover: redisList={}", this.redisList);

        List<JedisShardInfo> masterList = new ArrayList<>();
        List<JedisShardInfo> slaveList = new ArrayList<>();
        for (RedisConfig config : this.redisList) {
            masterList.add(config.getMaster());
            slaveList.add(config.getSlave() != null ? config.getSlave() : config.getMaster());
        }

        this.masterList = masterList;
        this.slaveList = slaveList;
        intitJedisPool(this.poolConfig, this.masterList, this.slaveList);

        return this.masterJedisPool.getResource();
    }

}
