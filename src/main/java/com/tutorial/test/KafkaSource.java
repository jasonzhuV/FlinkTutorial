package com.tutorial.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.Properties;

/**
 * 从kafka读取数据
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {

        // 1 获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 设置并行任务数量
        env.setParallelism(1);

        Properties properties = new Properties();

        // kafka 的配置类 ConsumerConfig 还有 ProducerConfig CommonClientConfig
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DataStreamSource<String> kafkaStream = env
                .addSource(new FlinkKafkaConsumer<String>(
                        "first",
                        new SimpleStringSchema(),
                        properties
                ));

        kafkaStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] arr = value.split(" ");
                        if (arr.length > 0 && !"".equals(value)) {
                            for (String word : arr) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> acc, Tuple2<String, Long> element) throws Exception {
                        return Tuple2.of(acc.f0, acc.f1 + element.f1);
                    }
                })
                .print(); // WordCount


        env.execute("test kafka");
    }

    @Test
    public void test1() {
        String line = "";
        String[] s = line.split(" ");
        System.out.println(s.length);
    }
}

