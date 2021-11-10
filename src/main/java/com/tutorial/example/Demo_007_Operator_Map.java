package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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

        /*
         * 1.1、匿名内部类的写法，还有匿名外部类
         */
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        }).print();

        /*
         * 1.2、外部类的写法
         */
        stream.map(new MyMap()).print();

        /*
         * 2、匿名函数的写法，如果有类型擦除不能类型推断，需要显式指定返回值的类型，这里可以，但是最好加上
         */
        stream
                .map(r -> r.user)
                .returns(Types.STRING)
                .print();

        /*
         * 3、、指定输入参数的类型，要显式指定返回值类型，因为不能自动推断
         * 这里的String能自动推断，但还是都加上
         */
        stream
                .map((Event event) -> event.user)
                .returns(Types.STRING)
                .print();

        /*
         * 4、匿名内部类的lambda写法
         * 可以不写输入参数的类型，但是前面的函数签名
         * TODO (MapFunction<Event, String>)
         * 已经起到了这样的作用
         */
        stream
                .map((MapFunction<Event, String>) event -> event.user)
                .returns(Types.STRING)
                .print();

        // use flatmap to implement map
        stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                collector.collect(event.user);
            }
        }).print();

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
