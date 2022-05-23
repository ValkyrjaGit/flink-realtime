package com.ytjj.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ytjj.bean.VisitorStats;
import com.ytjj.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 按照维度做轻度聚合
 * 聚合窗口:  10s
 * 聚合维度:  平台 渠道  新老用户
 * 聚合方式:  类似于离线数仓中的union
 * 度量值  :  启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        //检查点 CK 相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend("hdfs://dataserver001:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        String groupId = "visitor_stats_app";

        //TODO 1.从kafka中获取需要聚合的数据流
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);


        SingleOutputStreamOperator<VisitorStats> pageViewStatsDstream = pageViewDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"),
                    jsonObj.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDstream = uniqueVisitDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
        });
        //2.3 转换 sv 流
        SingleOutputStreamOperator<VisitorStats> sessionVisitDstream = pageViewDStream.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<VisitorStats> out)
                            throws Exception {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            // System.out.println("sc:"+json);
                            VisitorStats visitorStats = new VisitorStats("", "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));
                            out.collect(visitorStats);
                        }
                    }
                });
        //2.4 转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpStatDstream = userJumpDStream.map(json
                -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });

        DataStream<VisitorStats> unionDetailDstream = pageViewStatsDstream.union(uniqueVisitStatsDstream, sessionVisitDstream, userJumpStatDstream);
        unionDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        env.execute();


    }
}
