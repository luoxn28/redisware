package com.luo.redisware.config.raw;

import cn.hutool.core.collection.CollectionUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis实例配置，包括 master slave
 *
 * @author xiangnan
 * @date 2018/3/20 14:20
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RedisConfig {
    private GenericObjectPoolConfig poolConfig;

    private NodeConfig master;
    private List<NodeConfig> slaveList;

    /**
     * slave实例创建连接池
     */
    public static List<JedisPool> buildPoolList(GenericObjectPoolConfig pool, List<NodeConfig> nodeList) {
        if (CollectionUtil.isEmpty(nodeList)) {
            return new ArrayList<>();
        }

        List<JedisPool> poolList = new ArrayList<>();
        for (NodeConfig config : nodeList) {
            poolList.add(buildPool(pool, config));
        }
        return poolList;
    }

    /**
     * master实例创建连接池
     */
    public static JedisPool buildPool(GenericObjectPoolConfig pool, NodeConfig node) {
        if (node == null) {
            throw new RuntimeException("buildPool nodeConfig is null");
        }

        return new JedisPool(pool != null ? pool : new GenericObjectPoolConfig(),
                node.getHost(), node.getPort(), node.getTimeout(), node.getPassword());
    }
}
