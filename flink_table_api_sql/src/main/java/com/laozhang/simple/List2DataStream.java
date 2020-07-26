package com.laozhang.simple;

import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class List2DataStream {
	public static void main(String[] args) throws Exception {

		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);

		// 生成机器信息表 Machines
		List<Tuple3<String, String, String>> historyData = new ArrayList<>();
		historyData.add(Tuple3.of("10.132.183.222", "大连一厂","2020-03-04 12:43:50.562"));
		historyData.add(Tuple3.of("10.132.183.111", "昆山厂","2020-03-04 16:43:50.562"));
		historyData.add(Tuple3.of("10.132.183.111", "昆山分部","2020-03-04 20:43:50.562"));

		DataStream<Tuple3<String, String, String>> ratesHistoryStream = env.fromCollection(historyData);
		SingleOutputStreamOperator<Tuple3<String, String, String>> tuple3SingleOutputStreamOperator = ratesHistoryStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, String, String>>() {
			long currentMaxTimestamp = 0;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

			@SneakyThrows
			@Override
			public long extractTimestamp(Tuple3<String, String, String> data, long l) {
				long timestamp = sdf.parse(data.f2).getTime() + 28800000;
				currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
				return timestamp;
			}

			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(currentMaxTimestamp - 1500);
			}
		});
		fsTableEnv.createTemporaryView("MachineInfo",tuple3SingleOutputStreamOperator, "ip,area,updatetime");
		//fsTableEnv.createTemporaryView("MachineInfo", machineInfo);

//		TemporalTableFunction rates = machineInfo.createTemporalTableFunction("curtime", "ip");
//		fsTableEnv.registerFunction("Machines", rates);

		Table joinResult = fsTableEnv.sqlQuery("select * from MachineInfo");

		// 回缩流
		//DataStream<Tuple2<Boolean,Row>> stream = fsTableEnv.toRetractStream(joinResult, Row.class);
		// 追加流
		DataStream<Row> stream = fsTableEnv.toAppendStream(joinResult, Row.class);

		// 打印结果
		stream.print();
		env.execute("My Stream");
	}
}
