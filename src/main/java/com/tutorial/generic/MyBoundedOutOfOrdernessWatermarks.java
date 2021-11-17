package com.tutorial.generic;

public class MyBoundedOutOfOrdernessWatermarks<T> implements MyWatermarkGenerator<T> {

    private long maxTimestamp;

    public MyBoundedOutOfOrdernessWatermarks(String maxOutOfOrderness) {
        this.maxTimestamp = Long.MIN_VALUE + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, String output) {

    }

    @Override
    public void onPeriodicEmit(String output) {

    }
}
