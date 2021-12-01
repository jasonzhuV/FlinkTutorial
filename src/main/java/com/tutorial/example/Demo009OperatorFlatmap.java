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
 * @author     : zhupeiwen
 * 单流转换算子 ： 对一条流，并没有进行（分组）分流操作，还是一条流
 * flatmap ： 把每条数据转换成 0个一个或者多个，是map个filter的泛化
 * （flatmap是比map和filter更加底层的算子）
 * flatmap中使用集合的collect方法向下游发送数据
 */
public class Demo009OperatorFlatmap {

    static String WHITE_COLOR = "white";
    static String BLACK_COLOR = "black";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> dataStreamSource = env
                .fromElements("white", "gray", "black");

        /**
         * 1、匿名内部类 还有外部类
         */
        env
                .fromElements("white", "gray", "black")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        if (WHITE_COLOR.equals(s)) {
                            collector.collect(s);
                        } else if (BLACK_COLOR.equals(s)) {
                            collector.collect(s);
                            collector.collect(s);
                        }
                    }
                })
                .print();


        /**
         * 2、指定输入参数的类型，要显式指定返回值类型，因为不能自动推断
         */
        env
                .fromElements("white", "gray", "black")
                // TODO 这里的输入是个元组，但是要指定类型
                .flatMap((String s, Collector<String> collector) -> {
                    if (WHITE_COLOR.equals(s)) {
                        collector.collect(s);
                    } else if (BLACK_COLOR.equals(s)) {
                        collector.collect(s);
                        collector.collect(s);
                    }
                })
                // 类型注解
                .returns(Types.STRING)
                .print();


        /**
         * 3、匿名内部类的lambda写法，要显式指定返回值类型 returns(Types.STRING) ，因为不能自动推断
         */
        env
                .fromElements("white", "gray", "black")
                // 写了一个函数签名
                // TODO 可以不写元组的类型，但是要标注函数的泛型
                .flatMap((FlatMapFunction<String, String>) (s, collector) -> {
                    if (WHITE_COLOR.equals(s)) {
                        collector.collect(s);
                    } else if (BLACK_COLOR.equals(s)) {
                        collector.collect(s);
                        collector.collect(s);
                    }
                })
                // 类型注解
                .returns(Types.STRING)
                .print();

        env.execute();
    }

}
