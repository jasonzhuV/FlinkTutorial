package com.tutorial.session;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.tutorial.constants.SessionConstats;

/**
 * 会话窗口
 */
public class SessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
            .map(r -> Tuple2.of(r.split(",")[0], Long.parseLong(r.split(",")[1]) * 1000L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }))
            .keyBy(r -> r.f0)
            .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
                @Override
                public long extract(Tuple2<String, Long> stringLongTuple2) {
                    // dynamic gap return the time millisecond
                    return SessionConstats.gapConfig.get(stringLongTuple2.f0);
                }
            })).process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                @Override
                public void clear(Context context) throws Exception {
                    super.clear(context);
                }

                // TODO 窗口闭合时调用
                @Override
                public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements,
                    Collector<String> out) throws Exception {
                    long start = context.window().getStart();
                    long end = context.window().getEnd();
                    long currentWatermark = context.currentWatermark();
                    long count = elements.spliterator().getExactSizeIfKnown();
                    out.collect("窗口" + start + "---" + end + "共" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
                }
            }).print();

        env.execute();
    }

}
