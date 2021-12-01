package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.sql.Timestamp;

/**
 *
 * 所有的处理函数都继承自 {@link RichFunction}.
 *
 * <p>{@link org.apache.flink.streaming.api.functions.KeyedProcessFunction} 在keyBy之后使用
 */
public class Demo023ProcessFunctionKeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> true)
                .process(new MyKeyed())
                .print();

        env.execute();
    }
    // 第一个泛型：key的类型
    // 第二个泛型：输入数据的类型
    // 第三个泛型：输出数据的类型
    public static class MyKeyed extends KeyedProcessFunction<Boolean, String, String> {
        @Override
        public void processElement(String in, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据触发调用
            long currTime = context.timerService().currentProcessingTime(); // 当前机器时间
            // 注册一个定时器，数据来的机器时间的10s之后
            context.timerService().registerProcessingTimeTimer(currTime + 10 * 1000L);
            collector.collect("数据到了！当前机器时间是：" + new Timestamp(currTime));
        }

        // KeyedProcessFunction 处理的是每个支流，对每一条支流都有自己的定时器
        // 1. 定时器是注册在当前key对应的支流上的
        // 2. 针对一个key在某一个时间戳只能注册一个定时器
        // 3. 定时器就想累加器一样，针对当前key可见（TODO 定时器也是一个状态，保存检查点的时候把定时器也保存下来）
        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> out) throws Exception {
            // timestamp 参数就是注册的定时器的时间戳
            // 当机器时间到达timestamp，onTimer函数被触发执行
            super.onTimer(timestamp, context, out);
            out.collect("定时器触发了！触发时间是：" + new Timestamp(timestamp));
        }
    }
}

