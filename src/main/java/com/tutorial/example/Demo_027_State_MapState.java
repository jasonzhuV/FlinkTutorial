package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import com.tutorial.source.IntegerSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ListState
 */
public class Demo_027_State_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // The parallelism is set to 1 for easy printing

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<String, String>("user-url", Types.STRING, Types.STRING)
                        );
                    }

                    @Override
                    public void processElement(Event event, Context context, Collector<String> out) throws Exception {
                        mapState.put(event.user, event.url);
                        out.collect(event.user + mapState.get(event.user));
                    }
                })
                .print();

        env.execute();
    }
}

