package com.laozhang.detect.sql;

import com.alibaba.fastjson.JSONObject;
import com.laozhang.common.KafkaStreamBuilder;
import com.laozhang.common.StateMapping;
import com.laozhang.entity.AlarmDetail;
import com.laozhang.entity.MachineInfo;
import com.laozhang.entity.Product;
import com.laozhang.entity.RawStateData;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableDetectPattern {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);

		initInputStateStream(env,fsTableEnv);

		env.execute("My Stream");
	}

	// 初始化状态输入流
	private static void initInputStateStream(StreamExecutionEnvironment env,StreamTableEnvironment fsTableEnv){
		FlinkKafkaConsumer<String> consumer = new KafkaStreamBuilder("192.168.2.213:9092", "laozhang").topic("mystate_raw").build();
		DataStreamSource<String> dataStreamSource = env.addSource(consumer);
		DataStream<RawStateData> rawStateDataStream = dataStreamSource.map(data -> JSONObject.parseObject(data, RawStateData.class));

		KeyedStream<RawStateData, String> rawStateDataObjectKeyedStream = rawStateDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RawStateData>(Time.seconds(10)) {
			@Override
			public long extractTimestamp(RawStateData element) {
				return element.getTime();
			}
		}).keyBy(new KeySelector<RawStateData, String>() {
			@Override
			public String getKey(RawStateData rawStateData) throws Exception {
				return rawStateData.getIp() + rawStateData.getId();
			}
		});

		SingleOutputStreamOperator<AlarmDetail> oldState = rawStateDataObjectKeyedStream.map(new RichMapFunction<RawStateData, AlarmDetail>() {
			private ValueState<Integer> oldState;

			@Override
			public AlarmDetail map(RawStateData rawStateData) throws Exception {
				int oStateValue = oldState.value();
				String alarmNo, alarmType, alarmMsg, run, mp, sp, aut;

				String[] an = rawStateData.getAn();
				String[] am = rawStateData.getAm();

				alarmNo = an != null ? an[0] : "";
				alarmType = rawStateData.getAlarm();
				alarmMsg = am != null ? am[0] : "";

				run = rawStateData.getRun();
				mp = rawStateData.getMp();
				sp = rawStateData.getSp();
				aut = rawStateData.getAut();

				int state = StateMapping.getState(alarmNo, alarmType, alarmMsg, mp, sp, run, aut, oStateValue);

				AlarmDetail alarmDetail = new AlarmDetail();
				alarmDetail.setIp(rawStateData.getIp());
				alarmDetail.setId(rawStateData.getId());
				alarmDetail.setTime(rawStateData.getTime());
				alarmDetail.setDoor(rawStateData.getDoor());
				alarmDetail.setAlarm(alarmType);
				alarmDetail.setAlarmMsg(alarmMsg);
				alarmDetail.setState(state);
				oldState.update(state);
				return alarmDetail;
			}

			@Override
			public void open(Configuration config) {
				ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("oldState", Integer.class, 0);
				oldState = getRuntimeContext().getState(descriptor);
			}
		});

		String cols = "";
		String tableName = "stateDetail";
		fsTableEnv.createTemporaryView(tableName,oldState,cols);


	}
}
