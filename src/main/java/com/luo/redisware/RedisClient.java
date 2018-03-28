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

    public final static int READ_MODE_MASTER_ONLY  = 0;
    public final static int READ_MODE_MASTER_SLAVE = 1;
    public final static int READ_MODE_SLAVE_ONLY   = 2;

    private int readMode = READ_MODE_MASTER_ONLY;

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

    public RedisClient(List<RedisConfig> redisList, GenericObjectPoolConfig poolConfig) {
        this(redisList, poolConfig, READ_MODE_MASTER_ONLY, null);
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

        List<JedisShardInfo> masterList = new ArrayList<>();
        List<JedisShardInfo> slaveList = new ArrayList<>();
        for (RedisConfig config : redisList) {
            masterList.add(config.getMaster());
            slaveList.add(config.getSlave() != null ? config.getSlave() : config.getMaster());
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

    private ShardedJedis getJedis() {
        try {
            return this.masterJedisPool.getResource();
        } catch (Exception e) {
            logger.error("getJedis error, e={}", e);
            throw e;
        }
    }

    private ShardedJedis getSlaveJedis() {
        try {
            return this.slaveJedisPool.getResource();
        } catch (Throwable e) {
            logger.error("getJedis error, e={}", e);
            throw e;
        }
    }

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

    public String set(String key, String value, int expire) throws Exception {
        return set(key, SafeEncoder.encode(value), expire);
    }

    public String set(String key, byte[] value, int expire) throws Exception {
        ShardedJedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.setex(SafeEncoder.encode(key), expire, value);
        } catch (Exception e) {
            logger.error("set {}={} error, e={}", key, value, e);

            ShardedJedis tmpJedis = null;
            try {
                tmpJedis = sentinelFailover(e);
                return tmpJedis.setex(SafeEncoder.encode(key), expire, value);
            } catch (Exception ex) {
                logger.error("sentinel failover set {}={} error, e={}", key, value, e);
                throw e;
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

    public String get(String key) throws Exception {
        ShardedJedis jedis = null;
        try {
            jedis = getJedis(true);
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("get {} error, e={}", key, e);

            ShardedJedis tmpJedis = null;
            try {
                Boolean master = this.masterRead.get();
                tmpJedis = (master != null && master) ? getSlaveJedis() : getJedis();
                return tmpJedis.get(key);
            } catch (Exception e2) {
                logger.error("failover get {} error, e={}", key, e);
                throw e;
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

    private ShardedJedis sentinelFailover(Exception e) throws Exception {

        if ((e instanceof JedisConnectionException) && (e.getMessage() != null) &&
                !e.getMessage().contains("Read timed out")) {

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
                    List<String> masterList = jedis.sentinelGetMasterAddrByName(redisConfig.getSentinelName());
                    System.out.println("sentinel array: " + masterList);
                    if (masterList.contains(String.valueOf(slave.getPort())) &&
                            (masterList.contains(slave.getHost()) || masterList.contains("127.0.0.1"))) {
                        sentinelFailover = true;

                        // sentinel已经进行了主备切换，这里也跟着进行主备信息切换
                        redisConfig.setSlave(redisConfig.getMaster());
                        redisConfig.setMaster(slave);
                    }
                }
            }

            if (sentinelFailover) {
                logger.info("getJedis sentinelFailover: redisList={}", this.redisList);

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
            } else {
                throw e;
            }

        } else {
            throw e;
        }

    }

}
