package com.tutorial.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * RichFunction
 * 所有的flink函数类都有其Rich版本，富函数类一般是以抽象类的形式出现，例如RichMapFunction、RichFilterFunction、RichReduceFunction
 * 与常规函数类的不同：富函数类可以获取运行环境的上下文，并拥有一些生命周期方法，可以实现更复杂的功能
 */
public class Demo_013_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3)
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("生命周期开始");
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("生命周期结束");
                    }
                })
                .print();

        env.execute("Rich Test");
    }
}
