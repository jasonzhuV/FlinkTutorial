package com.tutorial.source;

import com.tutorial.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源中发送水位线
 */
public class ClickSourceWithWaterMark implements SourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Event e1 = Event.of("Mary", "./home", 1000L);
        Event e2 = Event.of("Mary", "./home", 2000L);
        Event e3 = Event.of("Mary", "./home", 3000L);

        // 自定义数据源中向下游发送水位线的话，不用 collect 用 collectWithTimestamp
        // 在自定义数据源中发送了水位线，后面就不能用 assignTimestampsAndWatermarks 了，只能选一个
        sourceContext.collectWithTimestamp(e1, e1.timestamp);
        sourceContext.emitWatermark(new Watermark(e1.timestamp - 1L)); // 发送水位线
        sourceContext.collectWithTimestamp(e2, e2.timestamp);
        sourceContext.emitWatermark(new Watermark(e2.timestamp - 1L));
        sourceContext.collectWithTimestamp(e3, e3.timestamp);
        sourceContext.emitWatermark(new Watermark(e3.timestamp - 1L));
        sourceContext.emitWatermark(new Watermark(10000L));
    }

    @Override
    public void cancel() {

    }
}