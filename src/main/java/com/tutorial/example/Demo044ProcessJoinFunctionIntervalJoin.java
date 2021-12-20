package com.tutorial.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author zhupeiwen
 * @date 2021/11/17 11:14 上午
 * @description: ProcessJoinFunction 基于间隔的join，第一条流的每一条数据都会和第二条流的一段时间内的数据进行join （先keyBy再intervalJoin）
 * @demo : 每个用户的点击 join 这个用户最近10分钟的浏览
 */
public class Demo044ProcessJoinFunctionIntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 点击
        SingleOutputStreamOperator<Tuple3<String, String, Long>> clickStream =
            env.fromElements(Tuple3.of("Mary", "click", 20 * 60 * 1000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                        @Override
                        public long extractTimestamp(Tuple3<String, String, Long> e, long l) {
                            return e.f2;
                        }
                    }));
        // 浏览
        SingleOutputStreamOperator<Tuple3<String, String, Long>> browseStream = env
            .fromElements(Tuple3.of("Mary", "browse", 15 * 60 * 1000L), Tuple3.of("Mary", "browse", 11 * 60 * 1000L),
                Tuple3.of("Mary", "browse", 9 * 60 * 1000L), Tuple3.of("Mary", "browse", 21 * 60 * 1000L))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> e, long l) {
                        return e.f2;
                    }
                }));

        // a -> b
        // (lower, higher) a join b 和 b join a 是对称的，看下面的时间间隔范围
        clickStream.keyBy(r -> r.f0).intervalJoin(browseStream.keyBy(r -> r.f0))
            .between(Time.minutes(-10), Time.minutes(5))
            .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                @Override
                public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right,
                    Context context, Collector<String> collector) throws Exception {
                    collector.collect(left + "=>" + right);
                }
            }).print();

        // b -> a
        // (-higher, -lower)
        browseStream.keyBy(r -> r.f0).intervalJoin(clickStream.keyBy(r -> r.f0))
            .between(Time.minutes(-5), Time.minutes(10))
            .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                @Override
                public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right,
                    Context context, Collector<String> collector) throws Exception {
                    collector.collect(right + "->" + left);
                }
            }).print();

        env.execute();
    }
}
