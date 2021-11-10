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
 * flatmap ： 把每条数据转换成 0个一个或者多个，是map个filter的泛化
 * （flatmap是比map和filter更加底层的算子）
 * flatmap中使用集合的collect方法向下游发送数据
 */
public class Demo_009_Operator_Flatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> dataStreamSource = env
                .fromElements("white", "gray", "black");

        // 匿名内部类 还有外部类
        env
                .fromElements("white", "gray", "black")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        if (s.equals("white")) {
                            collector.collect(s);
                        } else if (s.equals("black")) {
                            collector.collect(s);
                            collector.collect(s);
                        }
                    }
                })
                .print();


        env
                .fromElements("white", "gray", "black")
                .flatMap((String s, Collector<String> collector) -> { // 这里的输入是个元组，但是要要指定类型
                    if (s.equals("white")) {
                        collector.collect(s);
                    } else if (s.equals("black")) {
                        collector.collect(s);
                        collector.collect(s);
                    }
                })
                .returns(Types.STRING) // 类型注解
                .print();


        /*
         * 下面个是匿名内部类的lambda写法，但是要加返回值类型 returns(Types.STRING) ，否则会报错
         */
        env
                .fromElements("white", "gray", "black")
                // 写了一个函数签名
                .flatMap((FlatMapFunction<String, String>) (s, collector) -> { // 可以不写元组的类型，但是要标注函数的泛型
                    if (s.equals("white")) {
                        collector.collect(s);
                    } else if (s.equals("black")) {
                        collector.collect(s);
                        collector.collect(s);
                    }
                })
                .returns(Types.STRING) // 类型注解
                .print();

        env.execute();
    }

}
