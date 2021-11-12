package com.tutorial.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;


/**
 * 泛型是数据源中的类型
 * 模拟连续上升的Long型数字
 */
public class LongSource implements SourceFunction<Long> {
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        sourceContext.collect(1L);
        Thread.sleep(200);
        sourceContext.collect(2L);
        Thread.sleep(200);
        sourceContext.collect(3L);
        Thread.sleep(200);
        sourceContext.collect(4L);
        Thread.sleep(200);
        sourceContext.collect(5L);
        Thread.sleep(200);
        sourceContext.collect(6L);
        Thread.sleep(200);
        sourceContext.collect(7L);
        Thread.sleep(200);
    }

    @Override
    public void cancel() {
    }
}