package com.ytjj.function;


import com.alibaba.fastjson.JSONObject;
import com.ytjj.util.DimUtil;
import com.ytjj.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    //声明一个线程池对象
    private ThreadPoolExecutor threadPoolExecutor;

    //声明属性
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                String key = getKey(input);
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);
                if (dimInfo != null && dimInfo.size() > 0) {
                    try {
                        join(input, dimInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
}
