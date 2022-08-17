package com.flink.multiStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.execute();
    }
}
