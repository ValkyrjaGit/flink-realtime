package com.ytjj.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

            jedisPoolConfig.setMaxTotal(100); //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, "dataserver001", 6379, 1000);

            System.out.println("启动Redis连接池");
            return jedisPool.getResource();
        } else {
            System.out.println("连接池:" + jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }
}