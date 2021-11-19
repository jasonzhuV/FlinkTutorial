package com.tutorial.example;

import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.util.Collector;

/**
 * author     : zhupeiwen
 * date       : 2021/11/17 3:41 下午
 * description:
 * 所有的并行任务检查点都保存成功，才叫一次检查点保存成功
 * 新的检查点保存不成功，旧的检查点不删除，新的检查点保存成功会覆盖旧的检查点
 */
public class Demo_047_CheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        /*
        env
                .fromElements(1,2,3,4,5)
                .map(r -> Tuple2.of(r % 2, r))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy( t -> t.f0)
                // 直接用sum拿不到key
                .sum(1)
                .print().setParallelism(2);

         */

//        env
//                .fromElements(1,2,3,4,5)
//                .keyBy(e -> e % 2)
//                .process(new KeyedProcessFunction<Integer, Integer, String>() {
//                    @Override
//                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
//
//                    }
//                });
        env.
                fromElements(1,2,3,4,5)
                .keyBy(e -> e % 2)
                .sum(0)
                .print();

        env.execute();
    }
}
