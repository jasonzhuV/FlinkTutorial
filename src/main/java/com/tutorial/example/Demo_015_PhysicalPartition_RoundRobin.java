package com.tutorial.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区
 * 轮询分区 求余的操作
 */
public class Demo_015_PhysicalPartition_RoundRobin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .rebalance()
                .print()
                .setParallelism(2);// 这里不指定并行度，默认是机器的核数


        env.execute();
    }
}
