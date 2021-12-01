package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author     : zhupeiwen
 * 单流转换算子 ： 对一条流，并没有进行（分组）分流操作，还是一条流
 * filter ： 泛型只有一个，输入的类型
 */
public class Demo008OperatorFilter {

    /**
     * 用于过滤的用户
     */
    static String filterUser = "Mary";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.filter(r -> filterUser.equals(r.user)).print();

        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return filterUser.equals(event.user);
            }
        }).print();

        stream.filter(new MyFilter()).print();

        // 使用flatmap实现filter
        stream.flatMap(new FlatMapFunction<Event, Event>() {
            @Override
            public void flatMap(Event event, Collector<Event> collector) throws Exception {
                if (filterUser.equals(event.user)) {
                    collector.collect(event);
                }
            }
        }).print();

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return filterUser.equals(event.user);
        }
    }
}
