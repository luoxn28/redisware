# redisware

redisware 一个分布式 Redis 解决方案，环境要求jdk1.8+

## redisware特性

1. 支持分布式Redis部署，单个Redis实例之间没有任何状态关联。
2. 支持Redis节点主备部署方式，支持读写分离、读失败failover。
3. 支持Redis故障转移（借助redis-monitor）。
4. 支持Redis水平扩容（冷启动方式）。
5. 上手简单，封装了jedis。

