package com.tutorial.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * <p>迟到数据
 * <p>使用迟到数据更新窗口计算结果
 * <p>迟到数据处理方式
 * <p>  1、直接丢弃
 * <p>  2、将迟到数据发送到侧输出流
 * <p>  3、在水位线超过窗口结束时间的时候，可以触发窗口的计算，但是保留这个窗口一段时间，不销毁窗口，再等一等迟到数据
 * <p>  迟到数据来了继续更新窗口的计算结果，等的这段时间过了再销毁窗口
 *
 * <p> Q:最大延迟时间和允许迟到时间，分开设置为5 和直接把最大延迟时间设置为10
 * <p> A:能更快看到计算结果，更快触发窗口计算，后面用迟到数据更新窗口的计算结果
 */
public class Demo041LatenessAllowLateness {
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
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        // 当水位线到达窗口结束时间，会第一次触发窗口的计算，但是不销毁窗口
                        ValueState<Boolean> firstCalculate = context.windowState().getState(new ValueStateDescriptor<Boolean>("first-calculate", Types.BOOLEAN));
                        if (firstCalculate == null) {
                            out.collect("窗口第一次触发计算，共" + elements.spliterator().getExactSizeIfKnown());
                        } else {
                            out.collect("迟到数据来了，共" + elements.spliterator().getExactSizeIfKnown());
                        }
                    }
                });

        // 输出主流
        result.print();


        env.execute();
    }
}

