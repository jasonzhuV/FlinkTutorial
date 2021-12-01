package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * <p>窗口实现的原理
 * <p>使用KeyedProcessFunction模拟滚动窗口
 * <p>配合定时器 + 状态变量
 */
public class Demo038WindowPrincipe {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.timestamp;
                            }
                        }))
                .keyBy(event -> event.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    // key:窗口的开始时间 -> value:窗口中的pv
                    private MapState<Long, Long> mapState;
                    // 窗口的长度
                    private Long windowSize = 5 * 1000L;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, Long>("window-info", Types.LONG, Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(Event event, Context context, Collector<String> out) throws Exception {
                        long windowStart = event.timestamp - event.timestamp % windowSize; // 该元素所处窗口的开始时间
                        long windowEnd = windowStart + windowSize - 1L; // 该元素所处窗口的结束时间
                        context.timerService().registerEventTimeTimer(windowEnd); // 注册窗口闭合时间的定时器
                        if (mapState.contains(windowStart)) {
                            Long pv = mapState.get(windowStart);
                            mapState.put(windowStart, pv + 1L);
                        } else {
                            mapState.put(windowStart, 1L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        long start = timestamp + 1L - windowSize;
                        long end = timestamp + 1L; // 左闭又开，右侧 + 1
                        out.collect(ctx.getCurrentKey() + ":" + new Timestamp(start) + "~" + new Timestamp(end) + ":" + mapState.get(start));
                        mapState.remove(start); // 删除闭合的窗口数据
                    }
                })
                .print();

        env.execute();
    }
}

