package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import com.tutorial.source.WindowClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 全窗口聚合和增量聚合结合使用
 * 减少窗口缓存的数据量、获取窗口信息
 * 在窗口闭合的时候，增量聚合会把聚合结果发送给全窗口聚合函数
 */
public class Demo033WindowAllAndIncremental {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new ClickSource())
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
                /*
                增量聚合 + 全窗口聚合结合使用：获取窗口信息 + 节省内存空间
                获取窗口信息:增量聚合不能获取窗口信息，结合全窗口聚合，获取窗口信息
                节省内存空间:全窗口聚合保存整个窗口内数据，使用增量聚合提前执行聚合逻辑，节省存储空间）
                */
                .aggregate(new PvCount(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            // 这时迭代器里只有一个值，就是增量聚合发送过来的
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long pv = elements.iterator().next(); // 增量聚合和全窗口聚合结合使用，获取窗口信息 + 节省内存空间
            out.collect("用户" + s + "在窗口" + new Timestamp(start) + " ~ " + new Timestamp(end) + "的pv数据是：" + pv);
        }
    }

    // 支流中每个窗口只保留一个数据，节省存储空间 相当于在每条支流的每个窗口有一个状态变量
    // 每个窗口都有自己的累加器，第二个泛型是累加器的类型
    // 不能访问窗口信息
    public static class PvCount implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            // 创建累加器
            return 0L;
        }

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

