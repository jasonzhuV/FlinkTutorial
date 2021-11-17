package com.tutorial.generic;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

@PublicEvolving
@FunctionalInterface
public interface MyWatermarkGeneratorSupplier<T> extends Serializable {

    MyWatermarkGenerator<T> createWatermarkGenerator(MyContext myContext);


    interface MyContext {
        MyMetricGroup getMetricGroup();
    }
}
