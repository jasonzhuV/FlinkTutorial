package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import com.tutorial.source.SessionClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 会话窗口
 */
public class Demo_031_Window_All_Session {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new SessionClickSource())
                .assignTimestampsAndWatermarks(
                        // 相当于最大延迟时间设置为0
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(event -> event.user)
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long currentWatermark = context.currentWatermark();
                        long count = elements.spliterator().getExactSizeIfKnown();
                        out.collect("窗口" + start + "---" + end + "共" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
                    }
                })
                .print();

        env.execute();
    }

}

