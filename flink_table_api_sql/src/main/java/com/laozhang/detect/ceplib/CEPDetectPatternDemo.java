package com.laozhang.detect.ceplib;

import com.alibaba.fastjson.JSONObject;
import com.laozhang.common.KafkaStreamBuilder;
import com.laozhang.entity.RawStateData;
import org.apache.flink.cep.CEP;
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

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class CEPDetectPatternDemo {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, fsSettings);

		initInputStateStream(env,streamTableEnvironment);

		env.execute("My Stream");
	}

	// 初始化状态输入流
	private static void initInputStateStream(StreamExecutionEnvironment env,StreamTableEnvironment streamTableEnvironment){
		FlinkKafkaConsumer<String> consumer = new KafkaStreamBuilder("192.168.2.213:9092", "laozhang").topic("mystate_raw").build();
		DataStreamSource<String> dataStreamSource = env.addSource(consumer);
		DataStream<RawStateData> rawStateDataStream = dataStreamSource.map(data -> JSONObject.parseObject(data, RawStateData.class));

		//rawStateDataStream.print();
		SingleOutputStreamOperator<RawStateData> rawStateDataSingleOutputStreamOperator = rawStateDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RawStateData>(Time.seconds(10)) {
			@Override
			public long extractTimestamp(RawStateData element) {
				return element.getTime();
			}
		});


		SkipPastLastStrategy skipPastLastStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
		Pattern<RawStateData, RawStateData> condition = Pattern.<RawStateData>begin("alarmStart",skipPastLastStrategy).oneOrMore().where(new SimpleCondition<RawStateData>() {
			@Override
			public boolean filter(RawStateData rawStateData) throws Exception {
				return "ALarM".equals(rawStateData.getAlarm());
			}
		}).next("alarmHandle").oneOrMore().where(new SimpleCondition<RawStateData>() {
			@Override
			public boolean filter(RawStateData rawStateData) throws Exception {
				return !"ALarM".equals(rawStateData.getAlarm()) && "****(reset)".equals(rawStateData.getRun());
			}
		}).followedBy("alarmEnd").where(new SimpleCondition<RawStateData>() {
			@Override
			public boolean filter(RawStateData rawStateData) throws Exception {
				return "STaRT".equals(rawStateData.getRun());
			}
		});

		PatternStream<RawStateData> pattern = CEP.pattern(rawStateDataSingleOutputStreamOperator, condition);

		SingleOutputStreamOperator<Row> result = pattern.process(new PatternProcessFunction<RawStateData, Row>() {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			@Override
			public void processMatch(Map<String, List<RawStateData>> map, Context context, Collector<Row> collector) throws Exception {
				RawStateData alarmStart = map.get("alarmStart").get(0);
				RawStateData alarmHandle = map.get("alarmHandle").get(0);
				RawStateData alarmEnd = map.get("alarmEnd").get(0);

				collector.collect(Row.of(alarmStart.getIp(),alarmStart.getId(),sdf.format(alarmStart.getTime()),sdf.format(alarmHandle.getTime()),sdf.format(alarmEnd.getTime())));
			}
		});
		result.print();
	}
}
