package com.jason.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo_002_StreamingSocket {
    /*
     * 流数据的word count
     * 用socket模拟流数据
     */
    public static void main(String[] args) throws Exception{
        // 1 获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 设置并行任务数量
        env.setParallelism(1);
        // 3 读取数据源
        // 注意先启动 nc -lk 9999
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        // 4 map操作
        SingleOutputStreamOperator<Tuple2<String, Long>> mappedStream = stream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] arr = value.split(" ");
                        for (String word : arr) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                });
        // shuffle
        KeyedStream<Tuple2<String, Long>, String> keyedStream = mappedStream
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                });

        // reduce
        SingleOutputStreamOperator<Tuple2<String, Long>> reducedStream = keyedStream
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> acc, Tuple2<String, Long> element) throws Exception {
                        return Tuple2.of(acc.f0, acc.f1 + element.f1);
                    }
                });

        reducedStream.print();

        env.execute();

    }
}
