package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import com.tutorial.source.ClickSourceWithWaterMark;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * <p>自定义插入水位线的逻辑：周期性
 * <p>窗口的增量聚合和全窗口聚合结合使用
 * <p>每个用户在每个窗口的pv数据
 */
public class Demo_037_WM_UserDefine_Source {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new ClickSourceWithWaterMark())
                .keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long pv = elements.spliterator().getExactSizeIfKnown(); // 增量聚合和全窗口聚合结合使用，获取窗口信息 + 节省内存空间
                        out.collect("用户" + key + "在窗口" + new Timestamp(start) + " ~ " + new Timestamp(end) + "的pv数据是：" + pv);
                    }
                })
                .print();

        env.execute();
    }
}

