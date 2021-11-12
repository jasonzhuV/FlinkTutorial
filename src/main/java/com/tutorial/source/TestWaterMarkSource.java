package com.tutorial.source;

import com.tutorial.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 泛型是数据源中的类型
 * 模拟点击数据源，其中Event是定义好的POJO类
 */
public class TestWaterMarkSource implements SourceFunction<String> {
    private boolean running = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        long timestamp = 1;
        while (running) {
            String content = "test," + timestamp++;
            // 使用collect方法将数据发送出去
            sourceContext.collect(content);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}