package com.tutorial.source;

import com.tutorial.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 泛型是数据源中的类型
 * 模拟数据源， 5秒钟发送一个用户名
 */
public class UrlSource implements SourceFunction<String> {
    private boolean running = true;
    String[] urlArr = {"./home", "./cart", "./prod?id=1"};
    private Random random = new Random();

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(urlArr[random.nextInt(urlArr.length)]);
            Thread.sleep(5000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}