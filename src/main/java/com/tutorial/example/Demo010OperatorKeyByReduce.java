package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 多流转换
 * 每个用户的pv数据
 */
public class Demo010OperatorKeyByReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 键控流
        // 同一个key对应的数据一定在一个任务槽
        // 不同key的数据也可能在一个任务槽，但逻辑上是隔离开的
        KeyedStream<Event, String> keyedStream = stream.keyBy(r -> r.user);

        keyedStream
                .map(r -> Tuple2.of(r.user, 1L)) // 匿名函数的写法
                .returns(Types.TUPLE(Types.STRING, Types.LONG)); // 显式指定返回值类型，因为有类型擦除，不能自动类型推断

        keyedStream
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                })
                // 使用user进行分区
                .keyBy(r -> r.f0)
                // 每一个key对应一个自己的累加器，累加器是一个状态
                // 每来一条数据，更新一次对应的累加器，然后将数据丢弃掉（滚动聚合，规约聚合）
                // 将累加器的结果输出
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();


        env.execute();
    }

}
