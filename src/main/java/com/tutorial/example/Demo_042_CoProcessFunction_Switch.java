package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * author     : zhupeiwen
 * date       : 2021/11/17 11:14 上午
 * description: 通过开关流，认识 CoProcessFunction
 */
public class Demo_042_CoProcessFunction_Switch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventSource = env.addSource(new ClickSource());

        DataStreamSource<Tuple2<String, Long>> switchStream = env.fromElements(Tuple2.of("Mary", 10 * 1000L));

        eventSource
                .keyBy(r -> r.user)
                // 开关流也分流了，因为只向Mary这条流发送了开关，其他的流没有发送开关
                .connect(switchStream.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Event, Tuple2<String, Long>, Event>() {
                    private ValueState<Boolean> forwarding;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        forwarding = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("switch", Types.BOOLEAN));
                    }

                    @Override
                    public void processElement1(Event event, Context context, Collector<Event> collector) throws Exception {
                        if (forwarding.value() != null && forwarding.value()) {
                            collector.collect(event);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Long> in, Context context, Collector<Event> collector) throws Exception {
                        forwarding.update(true);
                        context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + in.f1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        forwarding.clear();
                    }
                })
                .print();

        env.execute();
    }
}
