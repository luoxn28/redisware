package com.luo.redisware.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * @author xiangnan
 * date 2018/4/22
 */
public class RediswareUtil {

    public static GenericObjectPoolConfig defaultPoolConfig() {
        GenericObjectPoolConfig defaultPoolConfig = new GenericObjectPoolConfig();

        // 初始化默认pool配置
        defaultPoolConfig.setMaxIdle(5);
        defaultPoolConfig.setMaxTotal(500);
        defaultPoolConfig.setMaxWaitMillis(2000);
        defaultPoolConfig.setBlockWhenExhausted(false);

        return defaultPoolConfig;
    }

}
