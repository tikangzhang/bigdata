package com.laozhang.detect.ceplib;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.laozhang.common.KafkaStreamBuilder;
import com.laozhang.entity.RawStateData;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CEPDetectPatternForCollection {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, fsSettings);

		initInputStateStream(env,streamTableEnvironment);

		env.execute("My Stream");
	}

	// 初始化状态输入流
	private static void initInputStateStream(StreamExecutionEnvironment env,StreamTableEnvironment streamTableEnvironment) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		List<Tuple3<String, Long, Integer>> historyData = new ArrayList<>();
		historyData.add(Tuple3.of("ALarM", sdf.parse("2020-03-04 10:00:00").getTime(),0));
		historyData.add(Tuple3.of("ALarM", sdf.parse("2020-03-04 10:00:01").getTime(),0));
		historyData.add(Tuple3.of("ALarM", sdf.parse("2020-03-04 10:00:02").getTime(),0));
		historyData.add(Tuple3.of("****(reset)", sdf.parse("2020-03-04 10:00:03").getTime(),0));
		historyData.add(Tuple3.of("****(reset)", sdf.parse("2020-03-04 10:00:04").getTime(),0));
		historyData.add(Tuple3.of("****(reset)", sdf.parse("2020-03-04 10:00:05").getTime(),1));
		historyData.add(Tuple3.of("****(reset)", sdf.parse("2020-03-04 10:00:06").getTime(),1));
		historyData.add(Tuple3.of("STaRT", sdf.parse("2020-03-04 10:00:07").getTime(),18));
		historyData.add(Tuple3.of("STaRT", sdf.parse("2020-03-04 10:00:08").getTime(),15));
		historyData.add(Tuple3.of("STaRT", sdf.parse("2020-03-04 10:00:00").getTime(),14));
		SingleOutputStreamOperator<Tuple3<String, Long, Integer>> tuple3SingleOutputStreamOperator = env.fromCollection(historyData)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.seconds(10)) {
			@SneakyThrows
			@Override
			public long extractTimestamp(Tuple3<String, Long, Integer> element) { return element.f1; }});

		SkipPastLastStrategy skipPastLastStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
		Pattern<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>> condition =
		Pattern.<Tuple3<String, Long, Integer>>begin("alarmStart",skipPastLastStrategy).times(1).where(new SimpleCondition<Tuple3<String, Long, Integer>>() {
			@Override
			public boolean filter(Tuple3<String, Long, Integer> rawStateData) throws Exception {
				return "ALarM".equals(rawStateData.f0);
			}
		}).followedBy("alarmHandle").oneOrMore().where(new SimpleCondition<Tuple3<String, Long, Integer>>() {
			@Override
			public boolean filter(Tuple3<String, Long, Integer> rawStateData) throws Exception {
				return "ALarM".equals(rawStateData.f0);
			}
		}).next("alarmHandle0").oneOrMore().where(new SimpleCondition<Tuple3<String, Long, Integer>>() {
			@Override
			public boolean filter(Tuple3<String, Long, Integer> rawStateData) throws Exception {
				return !"ALarM".equals(rawStateData.f0) && "****(reset)".equals(rawStateData.f0);
			}
		}).followedBy("alarmEnd").where(new SimpleCondition<Tuple3<String, Long, Integer>>() {
			@Override
			public boolean filter(Tuple3<String, Long, Integer> rawStateData) throws Exception {
				return "STaRT".equals(rawStateData.f0);
			}
		});

		PatternStream<Tuple3<String, Long, Integer>> pattern = CEP.pattern(tuple3SingleOutputStreamOperator, condition,new EventComparator<Tuple3<String, Long, Integer>>(){

			@Override
			public int compare(Tuple3<String, Long, Integer> o1, Tuple3<String, Long, Integer> o2) {
				return o1.f1.compareTo(o2.f1);
			}
		});

		SingleOutputStreamOperator<Row> result = pattern.process(new PatternProcessFunction<Tuple3<String, Long, Integer>, Row>() {
			@Override
			public void processMatch(Map<String, List<Tuple3<String, Long, Integer>>> map, Context context, Collector<Row> collector) throws Exception {
				System.out.println(JSON.toJSON(map).toString());
			}
		});
		result.print();
	}
}
