package com.laozhang.flink.datastream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        List<Tuple3<String,Long,Integer>> mockData = new LinkedList<>();
        mockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:00").getTime(),1));
        mockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:05").getTime(),4));
        mockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:10").getTime(),6));
        mockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:01").getTime(),2));
        mockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:06").getTime(),3));
        mockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:11").getTime(),5));


        List<Tuple3<String,Long,String>> otherMockData = new LinkedList<>();
        otherMockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:05").getTime(),"laoli"));
        otherMockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:05").getTime(),"laozhang"));


        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> tuple3SingleOutputStreamOperator = env.fromCollection(mockData).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Integer> o) {
                return o.f1;
            }
        });
        SingleOutputStreamOperator<Tuple3<String, Long, String>> otherRuple3SingleOutputStreamOperator = env.fromCollection(otherMockData).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, String> o) {
                return o.f1;
            }
        });
        tuple3SingleOutputStreamOperator.join(otherRuple3SingleOutputStreamOperator).where(line->line.f0).equalTo(line->line.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))).apply(new JoinFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, String>, Tuple4<String,Long,Integer,String>>() {
            @Override
            public Tuple4<String,Long,Integer,String> join(Tuple3<String, Long, Integer> a, Tuple3<String, Long, String> b) throws Exception {
                return Tuple4.of(a.f0,a.f1,a.f2,b.f2);
            }
        }).print();

        env.execute();
    }
}
