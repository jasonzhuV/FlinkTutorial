package com.tutorial.example;

import com.tutorial.source.IntegerSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用reduce来实现sum，min，max
 */
public class Demo_011_Operator_Reduce_MinMax {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .keyBy(new KeySelector<Integer, Boolean>() {
                    @Override
                    public Boolean getKey(Integer integer) throws Exception {
                        return true;
                    }
                })
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer i1, Integer i2) throws Exception {
                        return Math.max(i1, i2);
                    }
                })
                .print();

        env.execute();
    }
}