package com.tutorial.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * <p>迟到数据
 * <p>来的数据的时间戳小于当前水位线，就是迟到数据
 * <p>指定水位线的时候可以指定最大延迟时间，拨慢时钟
 * <p>当水位线到达（等于）窗口关闭时间，窗口闭合 窗口左闭右开 [1,5000) -> [1,4999] 水位线到达4999则窗口闭合
 */
public class Demo039LatenessDefault {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(",");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<String> collector) throws Exception {
                        if (stringLongTuple2.f1 < context.timerService().currentWatermark()) {
                            collector.collect("迟到数据来了！" + stringLongTuple2);
                        } else {
                            collector.collect("没有迟到的数据：" + stringLongTuple2);
                        }
                    }
                })
                .print();

        env.execute();
    }
}

