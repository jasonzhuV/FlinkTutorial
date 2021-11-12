package com.tutorial.source;

import com.tutorial.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 泛型是数据源中的类型
 * 模拟点击数据源，其中Event是定义好的POJO类
 */
public class IntegerSource implements SourceFunction<Integer> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        Random random = new Random();
        while (running) {
            sourceContext.collect(random.nextInt(10));
            Thread.sleep(500L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}