package com.tutorial.example;

import com.tutorial.source.IntegerSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 求整数流的平均值
 * 使用reduce实现，reduce的累加器思想，滚动聚合，规约聚合
 *
 * 对于单流转换算子 map flatmap filter 是无状态算子
 * 输入不变的情况下，输出一定是不变的
 *
 * 有状态的算子，输入相同的情况下，输出是变化的 像 reduce
 */
public class Demo_012_Operator_Reduce_Avg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.addSource(new IntegerSource());

        stream
                //匿名函数的写法，下面需要显式指定返回值类型，因为有类型擦除，不能推断类型
                .map(e -> Tuple2.of(1L, e.doubleValue()))
                .returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .keyBy(e -> true)
                // TODO 累加器的类型和流中元素的类型是一样的
                .reduce(new ReduceFunction<Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> reduce(Tuple2<Long, Double> acc, Tuple2<Long, Double> in) throws Exception {
                        // TODO 返回的是累加器
                        return Tuple2.of(acc.f0 + in.f0, acc.f1 + in.f1);
                    }
                })
                .map(new MapFunction<Tuple2<Long, Double>, Double>() {
                    @Override
                    public Double map(Tuple2<Long, Double> value) throws Exception {
                        return value.f1 / value.f0;
                    }
                })
                .print();

        env.execute();
    }

}
