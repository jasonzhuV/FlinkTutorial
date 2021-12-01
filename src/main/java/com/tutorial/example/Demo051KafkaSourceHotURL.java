package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.bean.UrlCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Properties;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/22 2:46 下午
 * <p>description:
 * <p>实时热门URL，每一个滚动窗口中访问量最大的前几个URL
 * <p>kafka source
 * <p>1.分流
 * <p>2.开窗
 * <p>3.增量聚合 + 全窗口聚合
 * <p>4.分流
 * <p>5.统计
 */
public class Demo051KafkaSourceHotURL {
    private final static Integer topN = 3;
    /* seconds of each tumbling window */
    private final static Integer tumblingWindowDuration = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topicName = "clicks";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        env.setParallelism(1);

        env
                .addSource(new FlinkKafkaConsumer<String>(
                        topicName,
                        new SimpleStringSchema(),
                        properties
                ))
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
                .keyBy(event -> event.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Event, Long, Long>() {
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
                        },
                        new ProcessWindowFunction<Long, UrlCount, String, TimeWindow>() {
                            @Override
                            public void process(String url, Context context, Iterable<Long> elements, Collector<UrlCount> out) throws Exception {
                                out.collect(new UrlCount(url, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
                            }
                        }
                )
                .keyBy(urlCount -> urlCount.windowEnd)
                .process(new KeyedProcessFunction<Long, UrlCount, String>() {
                    private ListState<UrlCount> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<UrlCount>("url-cnt", Types.POJO(UrlCount.class))
                        );
                    }

                    @Override
                    public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        // 每一个key只会注册一个定时器，因为一个key的窗口结束时间都是一样的
                        ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
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

                        for (int i = 0; i < (arrayList.size() < topN ? arrayList.size() : topN); i++) {
                            UrlCount tmp = arrayList.get(i);
                            result
                                    .append("访问量第").append(i + 1).append("的url：").append(tmp.url).append("，访问量：").append(tmp.cnt)
                                    .append("\n");
                        }
                        result
                                .append("================================================\n\n\n");
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }
}
