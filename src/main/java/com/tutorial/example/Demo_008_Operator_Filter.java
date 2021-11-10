package com.tutorial.example;

import com.tutorial.bean.Event;
import com.tutorial.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 单流转换算子 ： 对一条流，并没有进行（分组）分流操作，还是一条流
 * filter ： 泛型只有一个，输入的类型
 */
public class Demo_008_Operator_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        // 匿名函数的写法
        stream.map(r -> r.user).print();

        stream
                .map((Event event) -> event.user)
                .returns(Types.STRING)
                .print();

        // 匿名内部类的写法
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        }).print();

        env.execute("Operator Map");
    }

}
