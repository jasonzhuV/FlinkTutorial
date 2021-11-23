package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.bean.UrlCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/22 2:46 下午
 * <p>description:
 * <p>实时热门URL，每一个滚动窗口中访问量最大的前几个URL
 * <p>使用windowAll直接在流上开窗，再process(new ProcessAllWindowFunction)
 * <p>ProcessAllWindowFunction：可以处理直接在流上开窗的数据
 * <p>相当于把所有数据都发到一条流上(一个任务槽上)，然后再开窗，并行度是1
 *
 *
 * <ul>
 *   <li>{@link ProcessFunction}
 *   <li>{@link KeyedProcessFunction}
 *   <li>{@link CoProcessFunction}
 *   <li>{@link ProcessJoinFunction}
 *   <li>{@link BroadcastProcessFunction}
 *   <li>{@link KeyedBroadcastProcessFunction}
 *   <li>{@link ProcessWindowFunction}
 *   <ul>
 *       <li>{@link AggregateFunction}
 *       <li>{@link ReduceFunction}
 *   </ul>
 *   <li>{@link ProcessAllWindowFunction}
 * </ul>
 */
public class Demo_050_ProcessAllWindowFunction_HotURL {

    private final static Integer topN = 3;
    /* seconds of each tumbling window */
    private final static Integer tumblingWindowDuration = 10;

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
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Event, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
                        // 一条流放到一个任务槽中处理，数据量大的话，性能低
                        // 对比分流增量聚合再全窗口聚合的方式，后者更能充分体现分布式计算的优势
                        HashMap<String, Long> urlToCnt = new HashMap<>();
                        for (Event e : iterable) {
                            if (!urlToCnt.containsKey(e.url)) {
                                urlToCnt.put(e.url, 1L);
                            } else {
                                urlToCnt.put(e.url, urlToCnt.get(e.url) + 1L);
                            }
                        }

                        ArrayList<Tuple2<String, Long>> arrayList = new ArrayList<>();
                        for (String key : urlToCnt.keySet()) {
                            arrayList.add(Tuple2.of(key, urlToCnt.get(key)));
                        }
                        arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
                                return t2.f1.intValue() - t1.f1.intValue();
                            }
                        });

                        StringBuilder result = new StringBuilder();
                        result
                                .append("================================================\n")
                                .append("窗口结束时间：").append(new Timestamp(context.window().getEnd()))
                                .append("\n");

                        for (int i = 0; i < 2; i++) {
                            Tuple2<String, Long> tmp = arrayList.get(i);
                            result
                                    .append("第").append(i + 1).append("名的url是：").append(tmp.f0).append("，访问量是：").append(tmp.f1)
                                    .append("\n");
                        }
                        result
                                .append("================================================\n\n\n\n");
                        collector.collect(result.toString());

                    }
                })
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, UrlCount, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlCount> out) throws Exception {
            out.collect(new UrlCount(
                    s,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, UrlCount, String> {
        private Integer n;

        public TopN() {
        }

        public TopN(Integer n) {
            this.n = n;
        }

        private ListState<UrlCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlCount>("list-state", Types.POJO(UrlCount.class))
            );
        }

        // keyBy 之后，每一条流的元素：同一个窗口中，不同URL的访问量
        @Override
        public void processElement(UrlCount urlCount, Context ctx, Collector<String> out) throws Exception {
            listState.add(urlCount);
            ctx.timerService().registerEventTimeTimer(urlCount.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long winEnd = timestamp - 100L;
            long winStart = winEnd - tumblingWindowDuration * 1000L;
            ArrayList<UrlCount> arrayList = new ArrayList<>();
            for (UrlCount urlCount : listState.get()) {
                arrayList.add(urlCount);
            }
            listState.clear();

            arrayList.sort(new Comparator<UrlCount>() {
                @Override
                public int compare(UrlCount o1, UrlCount o2) {
                    return o2.cnt.intValue() - o1.cnt.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result
                    .append("================================================\n")
                    .append(new Timestamp(winStart)).append(" ~ ").append(new Timestamp(winEnd)).append("\n")
                    .append("================================================\n");

            for (int i = 0; i < (arrayList.size() < n ? arrayList.size() : n); i++) {
                UrlCount tmp = arrayList.get(i);
                result
                        .append("访问量第").append(i + 1).append("的url：").append(tmp.url).append("，访问量：").append(tmp.cnt)
                        .append("\n");
            }
            result
                    .append("================================================\n\n\n");
            out.collect(result.toString());
        }
    }
}
