package com.ytjj.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * 根据查询条件，使用phoenix去hbase中查询维表的数据
 */
public class DimUtil {
    private static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) {
        if (columnValues.length <= 0) {
            throw new RuntimeException("请至少设置一个条件！");
        }
        StringBuilder whereSql = new StringBuilder(" where ");

        StringBuilder redisKey = new StringBuilder(tableName).append(":");
        for (int i = 0; i < columnValues.length; i++) {
            Tuple2<String, String> columnValue = columnValues[i];
            String column = columnValue.f0;
            String value = columnValue.f1;
            whereSql.append(column).append("='").append(value).append("'");

            redisKey.append(value);

            if (i < columnValues.length - 1) {
                whereSql.append(" and ");
                redisKey.append(":");
            }
        }

        //根据条件先去redis中查询维度数据
        Jedis jedis = RedisUtil.getJedis();
        String dimJsonStr = jedis.get(redisKey.toString());

        //判断是否从Redis中查询到数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            jedis.close();
            return JSON.parseObject(dimJsonStr);
        }

        String querySql = "select * from " + tableName + whereSql.toString();
        System.out.println(querySql);

        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class);
        JSONObject dimJsonObj = queryList.get(0);

        //如果是从Hbase中查出来数据，那么把这个维度数据写入到redis中去
        jedis.set(redisKey.toString(), dimJsonObj.toString());
        jedis.expire(redisKey.toString(), 24 * 60 * 60);
        jedis.close();

        return dimJsonObj;
    }

    /**
     * 默认条件是查询id
     */
    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }

    /**
     * 如果维度表中的数据发生了改变，那么就要让对应的redis中的缓存失效
     */
    public static void deleteCached(String key) {
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(key);
        jedis.close();
    }

    public static void main(String[] args) {

    }

}
