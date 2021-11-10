package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 单流转换算子 ： 对一条流，并没有进行（分组）分流操作，还是一条流
 * map、flatmap、filter
 * map 对流中的每一个元素进行转换，泛型需要有输入和输出
 */
public class Demo_007_Operator_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        // 匿名函数的写法
        stream.map(r -> r.user).print();
        // 匿名内部类的写法
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        }).print();
        // 外部类的写法
        stream.map(new MyMap()).print();

        env.execute("Operator Map");
    }

    // 第一个泛型是输入的类型，第二个泛型是输出的类型
    public static class MyMap implements MapFunction<Event, String>{
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
