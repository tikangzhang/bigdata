package com.laozhang.flink.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class UnionThenWindowApply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Tuple3<String,Long,Integer>> mockData = new LinkedList<>();
        mockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:00").getTime(),1));
        mockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:05").getTime(),2));
        mockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:10").getTime(),4));
        mockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:01").getTime(),1));
        mockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:06").getTime(),9));
        mockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:11").getTime(),4));
        mockData.add(Tuple3.of("c",simpleDateFormat.parse("2020-03-12 08:00:02").getTime(),2));
        mockData.add(Tuple3.of("c",simpleDateFormat.parse("2020-03-12 08:00:07").getTime(),1));


        List<Tuple3<String,Long,Integer>> otherMockData = new LinkedList<>();
        otherMockData.add(Tuple3.of("a",simpleDateFormat.parse("2020-03-12 08:00:06").getTime(),1));
        otherMockData.add(Tuple3.of("b",simpleDateFormat.parse("2020-03-12 08:00:12").getTime(),2));


        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> tuple3SingleOutputStreamOperator = env.fromCollection(mockData).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Integer> o) {
                return o.f1;
            }
        });
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> otherRuple3SingleOutputStreamOperator = env.fromCollection(otherMockData).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Integer> o) {
                return o.f1;
            }
        });
        DataStream<Tuple3<String, Long, Integer>> union = tuple3SingleOutputStreamOperator.union(otherRuple3SingleOutputStreamOperator);

        union.keyBy((line)->line.f0).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple3<String, Long, Integer>, Tuple3<String,String,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Tuple3<String, Long, Integer>> iterable, Collector<Tuple3<String,String,Integer>> collector) throws Exception {
                        String firstTime = null;
                        int sum = 0;
                        for(Tuple3<String, Long, Integer> line : iterable){
                            if (firstTime == null){
                                firstTime = simpleDateFormat.format(line.f1);
                            }
                            sum += line.f2;
                        }
                        collector.collect(Tuple3.of(s,firstTime,sum));
                    }
                }).print();

        env.execute();
    }
}
