package com.ytjj.dwm;

import com.alibaba.fastjson.JSON;
import com.ytjj.bean.OrderDetail;
import com.ytjj.bean.OrderInfo;
import com.ytjj.bean.OrderWide;
import com.ytjj.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取 Kafka 订单和订单明细主题数据 dwd_order_info dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(orderInfoKafkaSource);

        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailKafkaDS = (DataStreamSource<String>) env.addSource(orderDetailKafkaSource);


        KeyedStream<OrderInfo, Long> keyedOrderInfoStream = orderInfoKafkaDS
                .map(jsonStr -> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] createTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(createTimeArr[0]);
                    orderInfo.setCreate_hour(createTimeArr[1]);
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long l) {
                                return element.getCreate_ts();
                            }
                        }))
                .keyBy(OrderInfo::getId);

        KeyedStream<OrderDetail, Long> keyedOrderDetailStream = orderDetailKafkaDS
                .map(item -> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    OrderDetail orderDetail = JSON.parseObject(item, OrderDetail.class);
                    orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }))
                .keyBy(OrderDetail::getOrder_id);

        //事实表与事实表之间的双流join  + -5秒之内的数据能关联上，其它的就会丢弃
        SingleOutputStreamOperator<OrderWide> orderWideDS = keyedOrderInfoStream
                .intervalJoin(keyedOrderDetailStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        env.execute();
    }
}
