package com.tutorial.generic;

public interface MyWatermarkGenerator<T> {


    void onEvent(T event, long eventTimestamp, String output);


    void onPeriodicEmit(String output);
}