package com.laozhang.flink.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SplitStreamAndSelect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String,Integer>> mockData = new LinkedList<>();
        mockData.add(Tuple2.of("a",1));
        mockData.add(Tuple2.of("a",2));
        mockData.add(Tuple2.of("a",3));
        mockData.add(Tuple2.of("b",1));
        mockData.add(Tuple2.of("b",2));
        mockData.add(Tuple2.of("c",0));

        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.fromCollection(mockData);
        SplitStream<Tuple2<String, Integer>> split = tuple2DataStreamSource.split(new OutputSelector<Tuple2<String, Integer>>() {
            @Override
            public Iterable<String> select(Tuple2<String, Integer> stringIntegerTuple2) {
                List<String> output = new ArrayList<String>();
                if ("a".equals(stringIntegerTuple2.f0)) {
                    output.add("xx");
                } else if ("b".equals(stringIntegerTuple2.f0)) {
                    output.add("oo");
                } else {
                    output.add("other");
                }
                output.add("total");
                return output;
            }
        });


        split.select("total").project(1,0).print();

        //split.select("xx").print();

        //split.select("oo").print();

        //split.select("other").print();

        env.execute();
    }
}
