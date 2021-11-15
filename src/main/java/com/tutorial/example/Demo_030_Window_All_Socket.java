package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.WindowClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * All Window Function
 * 此例子是socket无界流，关闭socket会在流的最后插入正无穷大的水位线，为了保证所有的窗口都闭合，所有的定时器都被触发
 */
public class Demo_030_Window_All_Socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .socketTextStream("localhost", 9999)
                .map(r -> Tuple2.of(r.split(",")[0], Long.parseLong(r.split(",")[1]) * 1000L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }
                ))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
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

