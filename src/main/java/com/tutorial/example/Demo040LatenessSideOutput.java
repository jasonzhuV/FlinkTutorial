package com.tutorial.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/**
 * <p>迟到数据
 * <p>侧输出流
 * <p>迟到数据处理方式
 * <p>  1、直接丢弃
 * <p>  2、将迟到数据发送到侧输出流
 * <p>  3、在水位线超过窗口结束时间的时候，可以触发窗口的计算，但是保留这个窗口一段时间，不销毁窗口，再等一等迟到数据
 * <p>  迟到数据来了继续更新窗口的计算结果，等的这段时间过了再销毁窗口
 */
public class Demo040LatenessSideOutput {

    // 侧输出流的名字：标签
    private static OutputTag<Tuple2<String, Long>> lateOutput = new OutputTag<Tuple2<String, Long>>("late-event"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        SingleOutputStreamOperator<String> result = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
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
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lateOutput)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long pv = elements.spliterator().getExactSizeIfKnown(); // 增量聚合和全窗口聚合结合使用，获取窗口信息 + 节省内存空间
                        out.collect("key" + key + "在窗口" + new Timestamp(start) + " ~ " + new Timestamp(end) + "的统计数据是：" + pv);
                    }
                });
        // 输出主流
        result.print();
        // 输出侧输出流，相当于把迟到数据发送到了另一条流
        result.getSideOutput(lateOutput).print();

        env.execute();
    }
}

