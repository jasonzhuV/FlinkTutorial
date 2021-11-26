package com.tutorial.example.uniquevisitor;

import com.tutorial.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.time.Duration;


/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/25 2:51 下午
 * <p>description: 使用布隆过滤器，减小去重时的内存压力
 */
public class Example_02_BloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/Users/jason/MyProjects/FlinkTutorial/src/main/resources/clicks.csv")
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return Event.of(arr[0], arr[1], Long.parseLong(arr[2]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long recordTimestamp) {
                                        return event.timestamp;
                                    }
                                })
                )
                // ① 因为要开窗口 ② 增量聚合和全窗口聚合结合使用 把其处理为KeyedStream
                .keyBy(e -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<Event, Tuple2<BloomFilter<String>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            // 预期用户数量100,误判率0.01
            return Tuple2.of(BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 100, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(Event event, Tuple2<BloomFilter<String>, Long> acc) {
            // 这里存在误判 可能来过的判断是不准确的，有可能某个值经过Hash之后，有命中，但是其他值的Hash覆盖的，也认为来过，就不加了
            if (!acc.f0.mightContain(event.user)) {
                acc.f0.put(event.user);
                acc.f1 += 1L; // uv值加一
            }
            return acc;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> acc) {
            return acc.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> bloomFilterLongTuple2, Tuple2<BloomFilter<String>, Long> acc1) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            String start = new Timestamp(context.window().getStart()).toString();
            String end = new Timestamp(context.window().getEnd()).toString();
            Long cnt = elements.iterator().next();
            out.collect(start + " ~ " + end + "的UV数据是：" + cnt);
        }
    }
}
