package com.tutorial.test;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
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

/**
 * @author 祝佩文
 *         <p>
 *         从kafka读取数据 在集群环境flink1.14之前，可以使用这种FlinkKafkaConsumer方式创建，之后就使用KafkaSource
 *         <p>
 *         https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/#kafka-source
 */
public class KafkaSourceTest {

    public static String topicName = "bcc_conversion_tag_slice";

    public static void main(String[] args) throws Exception {

        // 1 获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 设置并行任务数量
        env.setParallelism(1);

        Properties properties = new Properties();
        // kafka 的配置类 ConsumerConfig 还有 ProducerConfig CommonClientConfig
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-test-connection-1");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DataStreamSource<String> kafkaStream =
            env.addSource(new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), properties));

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
