package com.tutorial.source;

import com.tutorial.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 泛型是数据源中的类型
 * 模拟点击数据源，其中Event是定义好的POJO类
 */
public class WindowClickSource implements SourceFunction<Event> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        sourceContext.collect(Event.of("Mary", "./home", 1000L));
        sourceContext.collect(Event.of("Mary", "./home", 5000L));
        sourceContext.collect(Event.of("Mary", "./home", 9000L));
        sourceContext.collect(Event.of("Mary", "./home", 10000L));
        sourceContext.collect(Event.of("Mary", "./home", 11000L));
    }

    @Override
    public void cancel() {
        running = false;
    }
}