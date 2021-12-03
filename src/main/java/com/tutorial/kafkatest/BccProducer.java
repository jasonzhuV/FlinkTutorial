package com.tutorial.kafkatest;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author zhupeiwen
 * @date 2021/12/3 10:40 上午
 */
public class BccProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String bccTopicName = "bcc_conversion_tag_slice";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        env.readTextFile("/Users/jason/MyProjects/FlinkTutorial/src/main/resources/clicks.csv")
            .addSink(new FlinkKafkaProducer<String>(bccTopicName, new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                    return new ProducerRecord<>(bccTopicName, element.getBytes(StandardCharsets.UTF_8));
                }
            }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute();
    }
}
