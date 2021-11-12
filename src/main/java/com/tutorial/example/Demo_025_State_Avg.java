package com.tutorial.example;

import com.tutorial.source.IntegerSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedProcessFunction + state 求流上的平均值
 * KeyedProcessFunction 结合状态变量，还有定时器 可以实现map flatmap filter 以及 reduce
 *
 * 关于定时器的
 *      delete:取消某个时间戳的定时器，取消的是未来的定时器
 *      clear:清除状态变量，清除之后状态变量为null
 */
public class Demo_025_State_Avg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new IntegerSource())
                .keyBy(r -> true)
                .process(new Avg())
                .print();

        env.execute();
    }

    public static class Avg extends KeyedProcessFunction<Boolean, Integer, Double> {
        private ValueState<Tuple2<Integer, Integer>> valueState;
        private ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            valueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple2<Integer, Integer>>("sum-count", Types.TUPLE(Types.INT, Types.INT))
            );
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        @Override
        public void processElement(Integer value, Context context, Collector<Double> out) throws Exception {
            if (valueState.value() == null) {
                valueState.update(Tuple2.of(value, 1));
            } else {
                Tuple2<Integer, Integer> temp = valueState.value();
                valueState.update(Tuple2.of(temp.f0 + value, temp.f1 + 1));
            }
            if (timerTs.value() == null) {
                long timerTime = context.timerService().currentProcessingTime() + 3 * 1000L;
                context.timerService().registerProcessingTimeTimer(timerTime);
                timerTs.update(timerTime);
//                context.timerService().deleteProcessingTimeTimer(timerTime);
//                if ( timerTs != null ) {
//                    out.collect(99.88);
//                }
//                timerTs.clear();
//                if (timerTs.value() == null) {
//                    out.collect(88.99);
//                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Double> out) throws Exception {
            super.onTimer(timestamp, context, out);
            if (valueState.value() != null) {
                Integer sum = valueState.value().f0;
                Integer cnt = valueState.value().f1;
                out.collect((double) sum / cnt);
            }
            timerTs.clear();
        }
    }
}

