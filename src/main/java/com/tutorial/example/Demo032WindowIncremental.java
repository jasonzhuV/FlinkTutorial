package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.WindowClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Incremental Window Functions
 * 增量聚合函数
 * 每个用户在每个窗口的pv数据
 */
public class Demo032WindowIncremental {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new WindowClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 增量聚合
                .aggregate(new PvCount())
                .print();

        env.execute();
    }

    // 支流中每个窗口只保留一个数据，节省存储空间 相当于在每条支流的每个窗口有一个状态变量（ValueState） 对比全窗口聚合，是个ListState保存所有窗口数据
    // 每个窗口都有自己的累加器，第二个泛型是累加器的类型
    // 不能访问窗口信息
    public static class PvCount implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            // 创建累加器
            return 0L;
        }

        // TODO 每来一条数据就计算一次
        @Override
        public Long add(Event event, Long accumulator) {
            // 定义累加规则，每来一条数据+1
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            // 窗口闭合时，输出结果
            return accumulator;
        }

        // 此处没有合并窗口的需要
        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

}

