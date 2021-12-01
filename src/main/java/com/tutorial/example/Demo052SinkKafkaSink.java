package com.tutorial.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/23 3:03 下午
 * <p>description:
 */
public class Demo052SinkKafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topicName = "clicks";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

//        env
//                .readTextFile("/Users/jason/MyProjects/FlinkTutorial/src/main/resources/clicks.csv")
//                .addSink(new FlinkKafkaProducer<String>(
//                        topicName,
//                        new SimpleStringSchema(),
//                        properties
//                ));
        env
                .readTextFile("/Users/jason/MyProjects/FlinkTutorial/src/main/resources/clicks.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        topicName,
                        new KafkaSerializationSchema<String>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                                return new ProducerRecord<>(
                                        topicName,
                                        element.getBytes(StandardCharsets.UTF_8)
                                );
                            }
                        },
                        properties,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                ));

        env.execute();
    }
}
