package com.tutorial.dolphinscheduler;

import java.util.Calendar;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.tutorial.bean.Event;

/**
 * 泛型是数据源中的类型 模拟点击数据源，其中Event是定义好的POJO类
 * 
 * @author zhupeiwen
 */
public class DolphinSource implements SourceFunction<Event> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] userArr = {"Mary", "Bob", "Alice", "Jason", "Tom", "Jimmy"};
        String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2", "./prod?id=3"};
        while (running) {
            // 毫秒时间戳
            long currTs = Calendar.getInstance().getTimeInMillis();
            String username = userArr[random.nextInt(userArr.length)];
            String url = urlArr[random.nextInt(urlArr.length)];
            // 使用collect方法将数据发送出去
            sourceContext.collect(new Event(username, url, currTs));
            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}