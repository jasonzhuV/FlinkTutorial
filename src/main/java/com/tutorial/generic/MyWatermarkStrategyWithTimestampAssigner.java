package com.tutorial.generic;

public class MyWatermarkStrategyWithTimestampAssigner<T> implements MyWatermarkStrategy<T> {
    @Override
    public MyWatermarkGenerator<T> createWatermarkGenerator(MyContext myContext) {
        return null;
    }
}
