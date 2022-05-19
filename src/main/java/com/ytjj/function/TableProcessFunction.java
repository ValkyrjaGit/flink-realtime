package com.ytjj.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.ytjj.bean.TableProcess;
import com.ytjj.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> objectOutputTag;


    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.objectOutputTag = objectOutputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 核心逻辑 处理主流中的数据,将数据分流至kafka和hbase
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //读取配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String key = value.getString("tableName") + "-" + value.getString("type");

        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //kafka数据，写入主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //HBase数据，写入侧输出流
                ctx.output(objectOutputTag, value);
            }
        } else {
            System.out.println("该组合key:" + key + "不存在！");
        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columns.contains(next.getKey())) {
                iterator.remove();
            }
        }

    }

    /**
     * 处理配置表，将其处理成mapstate的格式
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //解析配置表的binlog信息，将其转换成TableProcess格式
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //获取信息之后，若该表在Hbase中不存在，则在Hbase中建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        //将数据写入状态，广播出去（会自动广播，不需要手动）
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);


    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key");
                } else {
                    createTableSQL.append(field).append(" varchar");
                }

                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")").append(sinkExtend);
            System.out.println(createTableSQL);
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("phoenix表" + sinkTable + "建表失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
