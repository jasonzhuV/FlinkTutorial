package com.tutorial.generic;

public class MyDataStream<T>{


    static <T> MyDataStream<T> getStreamInstance() {
        return new MyDataStream<T>();
    }

    public MySingleOutputStreamOperator<T> assignTimestampsAndWatermarks(MyWatermarkStrategy<T> myWatermarkStrategy) {

        return new MySingleOutputStreamOperator<>();

    }
}
