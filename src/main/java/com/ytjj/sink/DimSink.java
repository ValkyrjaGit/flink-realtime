package com.ytjj.sink;


import com.alibaba.fastjson.JSONObject;
import com.ytjj.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection connection=null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement=null;

        try {
            JSONObject data = value.getJSONObject("data");
            Set<String> keys = data.keySet();
            Collection<Object> values = data.values();

            //获取表名
            String tableName = value.getString("sink_table");

            String upsertSql = genUpsertSql(tableName, keys, values);
            System.out.println(upsertSql);
            preparedStatement = connection.prepareStatement(upsertSql);
            preparedStatement.executeUpdate();
            // TODO  如果是更新数据，那么还要去redis缓存中删除这条数据

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入phoenix维度数据失败");
        }finally {
            if(preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into "+ GmallConfig.HBASE_SCHEMA+"."+tableName+"("+StringUtils.join(keys,",")
                +")"+"values('"+StringUtils.join(values,"','")+"')";
    }
}
