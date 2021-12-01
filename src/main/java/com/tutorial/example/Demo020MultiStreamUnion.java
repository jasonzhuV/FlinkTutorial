package com.tutorial.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 分流 keyBy （或者物理分区）
 * 合流，将两条流合并在一起
 * union 算子
 *      ：将多条流合并在一起处理
 */
public class Demo020MultiStreamUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 1, 1);
        DataStreamSource<Integer> stream2 = env.fromElements(2, 2, 2);
        DataStreamSource<Integer> stream3 = env.fromElements(3, 3, 3);

        /*
         * 多条流合并成一条流
         * Q:谁先进入到这条流？顺序是怎么样的
         * A:以FIFO的方式进入合流，没有顺序，不去重
         *   可以合并多条流，但是每一条流的元素类型必须一样
         *   合并之后的流的类型不变
         */

        stream1.union(stream2, stream3).print();

        env.execute();
    }

}
