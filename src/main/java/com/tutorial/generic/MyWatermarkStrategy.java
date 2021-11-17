package com.tutorial.generic;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;


@Public
public interface MyWatermarkStrategy<T> extends MyWatermarkGeneratorSupplier<T> {
    static <T> MyWatermarkStrategy<T> forMonotonousTimestamps() {
        return (ctx) -> new MyAscendingTimestampsWatermarks<>();
    }


    default MyWatermarkStrategy<T> withTimestampAssigner(MySerializableTimestampAssigner<T> timestampAssigner) {
        return new MyWatermarkStrategyWithTimestampAssigner<>();
    }
}
