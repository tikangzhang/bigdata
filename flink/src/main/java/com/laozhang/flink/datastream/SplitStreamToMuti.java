package com.laozhang.flink.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SplitStreamToMuti {
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

        SingleOutputStreamOperator<Tuple3<String, String ,Integer>> map1 = tuple2DataStreamSource.map(new MapFunction<Tuple2<String, Integer>,Tuple3<String, String ,Integer>>(){
            @Override
            public Tuple3<String, String, Integer> map(Tuple2<String, Integer> record) throws Exception {
                return Tuple3.of("map1",record.f0,record.f1);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String ,Integer>> map2 = tuple2DataStreamSource.map(new MapFunction<Tuple2<String, Integer>,Tuple3<String, String ,Integer>>(){
            @Override
            public Tuple3<String, String, Integer> map(Tuple2<String, Integer> record) throws Exception {
                return Tuple3.of("map2",record.f0,record.f1);
            }
        });

        map1.print();
        map2.print();
        env.execute();
    }
}
