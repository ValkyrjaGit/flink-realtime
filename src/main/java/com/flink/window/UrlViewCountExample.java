package com.flink.window;

import com.flink.beans.Event;
import com.flink.beans.UrlViewCount;
import com.flink.datastream.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class UrlViewCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.keyBy(data -> data.url)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();


        env.execute();
    }

    private static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        //初始化累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        //累加器累加逻辑
        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        //输出结果
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        //累加器合并逻辑
        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    private static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(new UrlViewCount(s, elements.iterator().next(), new Timestamp(start), new Timestamp(end)));
        }
    }
}
