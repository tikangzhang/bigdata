package com.laozhang.detect.sql;

import com.laozhang.entity.RawStateData;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class List2DataStreamDetectPattern {
	public static void main(String[] args) throws Exception {

		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// 生成机器信息表 Machines
//		List<Tuple4<String, Long, Integer,Integer>> historyData = new ArrayList<>();
//		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:00").getTime(),2,1));
//		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:01").getTime(),2,2));
//		historyData.add(Tuple4.of("ABME", sdf.parse("2020-03-04 10:00:02").getTime(),4,1));
//		historyData.add(Tuple4.of("ABME", sdf.parse("2020-03-04 10:00:03").getTime(),4,3));
//		historyData.add(Tuple4.of("ADME", sdf.parse("2020-03-04 10:00:04").getTime(),6,2));
//		historyData.add(Tuple4.of("ADME", sdf.parse("2020-03-04 10:00:05").getTime(),6,1));
//		historyData.add(Tuple4.of("ADME", sdf.parse("2020-03-04 10:00:06").getTime(),6,1));
		List<Tuple4<String, Long, Integer,Integer>> historyData = new ArrayList<>();
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:00").getTime(),12,1));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:01").getTime(),17,2));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:02").getTime(),19,1));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:03").getTime(),21,3));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:04").getTime(),25,2));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:05").getTime(),18,1));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:06").getTime(),15,1));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:07").getTime(),14,1));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:08").getTime(),24,1));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:09").getTime(),25,1));
		historyData.add(Tuple4.of("ACME", sdf.parse("2020-03-04 10:00:10").getTime(),19,1));

		SingleOutputStreamOperator<Tuple4<String, Long, Integer,Integer>> tuple3SingleOutputStreamOperator = env.fromCollection(historyData).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<String, Long, Integer,Integer>>(Time.seconds(10)) {
			@SneakyThrows
			@Override
			public long extractTimestamp(Tuple4<String, Long, Integer,Integer> element) { return element.f1; }});
		fsTableEnv.createTemporaryView("Ticker",tuple3SingleOutputStreamOperator, "symbol,rowtime.rowtime,price,tax");

		Table result = fsTableEnv.sqlQuery("SELECT *\n" +
				"FROM Ticker\n" +
				"    MATCH_RECOGNIZE (\n" +
				"        PARTITION BY symbol\n" +
				"        ORDER BY rowtime\n" +
				"        MEASURES\n" +
				"            START_ROW.rowtime AS start_tstamp,\n" +
				"            PRICE_DOWN.rowtime AS bottom_tstamp,\n" +
				"            PRICE_UP.rowtime AS end_tstamp\n" +
				"        ONE ROW PER MATCH\n" +
				"        AFTER MATCH SKIP TO LAST PRICE_UP\n" +
				"        PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)\n" +
				"        DEFINE\n" +
				"            PRICE_DOWN AS\n" +
				"                (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR\n" +
				"                    PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),\n" +
				"            PRICE_UP AS\n" +
				"                PRICE_UP.price > LAST(PRICE_DOWN.price, 1)\n" +
				"    ) MR");

		//Table selectResult = fsTableEnv.sqlQuery("select * from Ticker");
		// 回缩流
		//DataStream<Tuple2<Boolean,Row>> stream = fsTableEnv.toRetractStream(joinResult, Row.class);
		// 追加流
		DataStream<Row> stream = fsTableEnv.toAppendStream(result, Row.class);

		// 打印结果
		stream.print();
		env.execute("My Stream");


		//Table result = fsTableEnv.sqlQuery("select * from Ticker");

	}
}
