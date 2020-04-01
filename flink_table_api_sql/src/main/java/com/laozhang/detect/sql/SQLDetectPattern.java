package com.laozhang.detect.sql;

import com.alibaba.fastjson.JSONObject;
import com.laozhang.common.KafkaStreamBuilder;
import com.laozhang.common.StateMapping;
import com.laozhang.entity.AlarmDetail;
import com.laozhang.entity.RawStateData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.core.JsonStreamContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.types.Row;

public class SQLDetectPattern {
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

		String cols = "state, ip, id, time, aut, run, alarm, mp, sp, cnt, an, am, door, tml, tul,rowtime.rowtime";
		String tableName = "state_detail";
		streamTableEnvironment.createTemporaryView(tableName,rawStateDataSingleOutputStreamOperator,cols);

//		Table selectResult = streamTableEnvironment.sqlQuery("SELECT run,alarm from state_detail where run ='STOP'");
//		selectResult.printSchema();
//		DataStream<Row> stream = streamTableEnvironment.toAppendStream(selectResult, Row.class);
//		stream.print();

		Table detectResult = streamTableEnvironment.sqlQuery("SELECT *\n" +
				"FROM state_detail\n" +
				"    MATCH_RECOGNIZE (\n" +
				"        PARTITION BY ip\n" +
				"        ORDER BY rowtime\n" +
				"        MEASURES\n" +
				"			 FIRST(A.rowtime) AS xone,\n" +
				"            LAST(A.rowtime) AS xtwo,\n" +
				"            FIRST(B.rowtime) AS xthree,\n" +
				"            LAST(B.rowtime) AS xfour,\n" +
				"            C.rowtime AS alarmEnd\n" +
//				"			 B.time - A.time AS responseTime,\n" +
//				"			 C.time - B.time AS handleTime\n" +
				"        ONE ROW PER MATCH\n" +
				"        AFTER MATCH SKIP PAST LAST ROW\n" +
				"        PATTERN (A+ B+ C) WITHIN INTERVAL '12' HOUR\n" +
				"        DEFINE\n" +
				"            A AS A.alarm = \'ALarM\', \n" +
				"            B AS B.run = \'****(reset)\' and B.alarm <> \'ALarM\',\n" +
				"            C AS C.run = \'STaRT\' " +
//				"            A AS A.cnt = 235, \n" +
//				"            B AS B.cnt = 236, \n" +
//				"            C AS C.cnt = 237 " +
				"    ) MR");
		DataStream<Row> astream = streamTableEnvironment.toAppendStream(detectResult, Row.class);
		astream.print();
	}
}
