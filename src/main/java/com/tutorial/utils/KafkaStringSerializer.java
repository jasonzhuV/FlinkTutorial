package com.tutorial.utils;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @author : zhupeiwen
 * @date : 2021/12/1 4:20 下午
 */
@NoArgsConstructor
@AllArgsConstructor
public class KafkaStringSerializer<T> implements KafkaSerializationSchema<KafkaProducerRecord<T>> {

    private String topicName;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaProducerRecord<T> record, @Nullable Long timestamp) {
        return new ProducerRecord<>(topicName, record.getPartition(), null,
            record.getKey() == null ? null : record.getKey().getBytes(StandardCharsets.UTF_8),
            Objects.requireNonNull(BaseJsonUtil.toJsonStr(record.getValue())).getBytes(StandardCharsets.UTF_8));
    }
}
