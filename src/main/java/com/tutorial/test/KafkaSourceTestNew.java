package com.tutorial.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhupeiwen
 * @date 2021/12/7 7:21 下午
 */
public class KafkaSourceTestNew {
    public static String cbbTopicName = "cbb_conversion_tag_slice";
    public static String brokerList = "10.20.0.40:9092";
    public static String groupId = "consumer-group-test-connection-1";

    public static void main(String[] args) throws Exception {

        // 1 获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 设置并行任务数量
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(brokerList)
            .setTopics(cbbTopicName).setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        // 这里注意版本变化，addSource 不能用了
        DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

        kafkaStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.trim().length() != 0;
            }
        }).flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> acc, Tuple2<String, Long> element)
                throws Exception {
                return Tuple2.of(acc.f0, acc.f1 + element.f1);
            }
        }).print(); // WordCount

        env.execute("test kafka");
    }
}
