package com.luo.redisware.config.raw;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Redis节点配置
 *
 * @author xiangnan
 * @date 2018/3/19 20:09
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeConfig {
    private String host;
    private int    port;
    private String password;
    private int    timeout = 2000; // default 2000ms
}
