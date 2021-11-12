package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 *
 * 事件时间语义的钟表 : 逻辑时钟 -> 水位线
 * <p>水位线是事件时间世界里的钟表，在事件时间的世界中，想要知道现在几点，看水位线的大小
 * <p>Flink认为时间戳小于等于水位线的事件都已到达
 * <p>水位线由编程人员设置，插入到数据源中，跟着数据向下流动
 * <p>水位线 = 观察到的事件中的最大时间戳 - 最大延迟时间 - 1ms
 * <p>水位线是一个特殊的事件
 */
public class Demo_028_Watermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L); // 时间戳 毫秒单位
                    }
                })
                // 分配水位线和时间线
                // 当有事件来的时候，每来一次事件就更新水位线，但是水位线没有插入，机器默认每隔200ms向数据流中插入一次水位线
                .assignTimestampsAndWatermarks(
                        // 设置最大延迟时间是5秒
                        // 水位线 = 观察到的事件中的最大时间戳 - 最大延迟时间 - 1ms
                        // forBoundedOutOfOrderness 乱序的
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
//                                System.out.println(recordTimestamp);
                                // 告诉flink数据中的哪一个字段是事件时间
                                return element.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> in, Context context, Collector<String> out) throws Exception {
                        context.timerService().registerEventTimeTimer(in.f1 + 5 * 1000L);
                        out.collect("watermark : " + context.timerService().currentWatermark());
                        System.out.println(context.timestamp());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了，触发时间：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}

