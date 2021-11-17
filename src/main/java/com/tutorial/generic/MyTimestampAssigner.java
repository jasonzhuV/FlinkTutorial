package com.tutorial.generic;

import org.apache.flink.annotation.Public;

@Public
@FunctionalInterface
public interface MyTimestampAssigner<T> {
    long extractTimestamp(T element, long recordTimestamp);
}
