package com.laozhang.flink.datastream;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

public class ConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple2<String,Integer>> mockData = new LinkedList<>();
        mockData.add(Tuple2.of("a",1));
        mockData.add(Tuple2.of("a",2));
        mockData.add(Tuple2.of("a",3));
        mockData.add(Tuple2.of("b",1));

        List<Tuple3<String,Long,Integer>> otherMockData = new LinkedList<>();
        otherMockData.add(Tuple3.of("a",100L,10));
        otherMockData.add(Tuple3.of("a",200L,20));
        otherMockData.add(Tuple3.of("b",300L,100));

        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.fromCollection(mockData);
        DataStreamSource<Tuple3<String, Long, Integer>> tuple3DataStreamSource = env.fromCollection(otherMockData);
        tuple2DataStreamSource.connect(tuple3DataStreamSource).map(new CoMapFunction<Tuple2<String, Integer>, Tuple3<String, Long, Integer>, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2;
            }

            @Override
            public Tuple2<String, Integer> map2(Tuple3<String, Long, Integer> stringLongIntegerTuple3) throws Exception {
                return Tuple2.of(stringLongIntegerTuple3.f0,stringLongIntegerTuple3.f1.intValue());
            }
        }).keyBy(line->line.f0).reduce((a,b)-> Tuple2.of(a.f0,a.f1 + b.f1)).print();

        env.execute();
    }
}
