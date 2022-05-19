package com.ytjj.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) {
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

            if (i < columnValues.length - 1) {
                whereSql.append(" and ");
            }
        }

        String querySql = "select * from " + tableName + whereSql.toString();
        System.out.println(querySql);

        List<JSONObject> queryList = PhoenixUtil.queryList(querySql, JSONObject.class);
        JSONObject dimJsonObj = queryList.get(0);
        return dimJsonObj;
    }

    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }

    public static void main(String[] args) {

    }

}
