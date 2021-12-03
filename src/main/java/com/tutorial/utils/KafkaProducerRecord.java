package com.tutorial.utils;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author : zhupeiwen
 * @date : 2021/12/1 6:49 下午
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProducerRecord<T> implements Serializable {
    private Integer partition;
    private String key;
    private T value;

    public static <E> KafkaProducerRecord<E> of(E value) {
        KafkaProducerRecord<E> record = new KafkaProducerRecord<>();
        record.setValue(value);
        return record;
    }

    public static <E> KafkaProducerRecord<E> of(String key, E value) {
        KafkaProducerRecord<E> record = new KafkaProducerRecord<>();
        record.setKey(key);
        record.setValue(value);
        return record;
    }

    public static <E> KafkaProducerRecord<E> of(Integer partition, String key, E value) {
        KafkaProducerRecord<E> record = new KafkaProducerRecord<>();
        record.setPartition(partition);
        record.setKey(key);
        record.setValue(value);
        return record;
    }
}
