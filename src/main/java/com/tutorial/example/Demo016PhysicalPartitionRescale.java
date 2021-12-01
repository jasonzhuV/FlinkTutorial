package com.tutorial.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区
 * Round-Robin不同，Round-Robin面向下游所有任务
 * Rescale只面向下游部分并行任务
 */
public class Demo016PhysicalPartitionRescale {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .map(r -> r)
                .setParallelism(2) // 上游算子的并行度2，就是说上游任务（数据发送方）数量是2
                .rescale()
                .print()
                .setParallelism(4); // 下游任务（数据接收方）的数量是4

        env.execute();
    }
}
