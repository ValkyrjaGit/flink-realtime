package com.ytjj.dwm;

import com.alibaba.fastjson.JSON;
import com.ytjj.bean.UserBehavior;
import com.ytjj.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


/**
 * 根据用户的行为数据得到uv数据，写入到kafka
 */
public class UserBehaviorApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer("app_user_behavior", "realtime");
        DataStream<String> behaviorDS = env.addSource(kafkaConsumer);


        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = behaviorDS.map(item ->
        {
            return JSON.parseObject(item, UserBehavior.class);
        });

        //过滤掉userId为空的数据，并且按照userid分组
        KeyedStream<UserBehavior, String> keyedUserBehaviorStream = userBehaviorDS
                .filter(item -> {
                    String userId = item.getUserId();
                    return StringUtils.isNotBlank(userId) && StringUtils.isNotEmpty(userId);
                })
                .keyBy(UserBehavior::getUserId);

//        keyedUserBehaviorStream.print();


        SingleOutputStreamOperator<UserBehavior> filterBehaviorDS = keyedUserBehaviorStream.filter(new RichFilterFunction<UserBehavior>() {
            private ValueState<String> firstValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);
                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                firstValueState = getRuntimeContext().getState(stringValueStateDescriptor);
            }


            @Override
            public boolean filter(UserBehavior value) throws Exception {
                String dateState = firstValueState.value();
                String cur_date = value.getCur_date();
                if (StringUtils.isEmpty(dateState) || !dateState.equals(cur_date)) {
                    firstValueState.update(cur_date);
                    return true;
                } else {
                    return false;
                }
            }
        });

        filterBehaviorDS.print();
        // TODO  将数据写入到kafka中
        env.execute();
    }
}
