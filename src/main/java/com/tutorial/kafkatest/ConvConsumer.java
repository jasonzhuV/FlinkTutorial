package com.tutorial.kafkatest;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author zhupeiwen
 * @date 2021/12/3 10:44 上午
 */
public class ConvConsumer {

    private static OutputTag<String> esOutput = new OutputTag<String>("data-to-es") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bccTopicName = "bcc_conversion_tag_slice";
        String vocDbTopicName = "voc_conversion_etl4db";
        String vocEsTopicName = "voc_conversion_etl4es";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        env.setParallelism(1);

        SingleOutputStreamOperator<String> dbStream =
            env.addSource(new FlinkKafkaConsumer<String>(bccTopicName, new SimpleStringSchema(), properties))
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("[DB] " + value);
                        ctx.output(esOutput, "[ES] " + value);
                    }
                });

        DataStream<String> esOutput = dbStream.getSideOutput(ConvConsumer.esOutput);

        dbStream.print();

        esOutput.print();

        // env.addSource(new FlinkKafkaConsumer<String>(bccTopicName, new SimpleStringSchema(), properties))
        // .addSink(new FlinkKafkaProducer<String>(vocDbTopicName, new KafkaSerializationSchema<String>() {
        // @Override
        // public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        // return new ProducerRecord<>(vocDbTopicName, element.getBytes(StandardCharsets.UTF_8));
        // }
        // }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute();
    }
}
