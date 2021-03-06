package com.tutorial.example;

import com.tutorial.source.IntegerSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhupeiwen
 * ListState
 */
public class Demo026StateListState {
    static Integer ODD_EVEN = 2;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // The parallelism is set to 1 for easy printing
        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .keyBy(r -> r % 2)
                .process(new KeyedProcessFunction<Integer, Integer, Double>() {
                    private ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(
                          new ListStateDescriptor<Integer>("integer", Types.INT)
                        );
                    }

                    @Override
                    public void processElement(Integer in, Context context, Collector<Double> out) throws Exception {
                        listState.add(in);
                        Integer sum = 0;
                        Integer cnt = 0;
                        for (Integer i : listState.get()) {
                            sum += i;
                            cnt += 1;
                        }
                        if (context.getCurrentKey() % ODD_EVEN == 0) {
                            // ?????????
                            out.collect((double) sum / cnt);
                        } else {
                            // ?????????
                        }
                    }
                })
                .print();
        env.execute();
    }
}

