package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.WindowClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * All Window Functions 全窗口函数：收集窗口中所有的数据
 * 此例子是有界流
 * 分流，开窗
 * 分流后，不同的KeyedStream可能被分到不同的任务槽甚至不同的机器上，所以开窗是针对每一个KeyedStream而言的
 */
public class Demo_029_Window_All_Bounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new WindowClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.timestamp;
                            }
                        })
                )
                .keyBy(event -> event.user)
                // 滚动窗口
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 滑动窗口
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    // 全窗口聚合函数：收集窗口中的所有数据
                    @Override
                    public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        // 迭代器中包含窗口中所有数据
                        // 当窗口闭合的时候调用process函数
                        // 当水位线到达窗口结束时间的时候，窗口闭合 TODO 有界流结束，当前窗口也闭合 流的结束插入正无穷大的水位线，保证所有的窗口都闭合，所有的定时器都被触发
                        long winStart = context.window().getStart();
                        long winEnd = context.window().getEnd();
                        long count = elements.spliterator().getExactSizeIfKnown();
                        out.collect("窗口" + new Timestamp(winStart) + "~" + new Timestamp(winEnd) + "共" + count + "条数据");
                    }
                })
                .print();

        env.execute();
    }

}

