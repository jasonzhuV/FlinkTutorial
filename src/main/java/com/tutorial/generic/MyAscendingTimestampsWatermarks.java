package com.tutorial.generic;

import org.apache.flink.annotation.Public;

@Public
public class MyAscendingTimestampsWatermarks<T> extends MyBoundedOutOfOrdernessWatermarks<T> {

    public MyAscendingTimestampsWatermarks() {
        super("1");
    }
}
