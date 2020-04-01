package com.laozhang.join;

import com.alibaba.fastjson.JSONObject;
import com.laozhang.entity.MachineInfo;
import com.laozhang.entity.Product;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
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

public class TableSqlJoin {
	public static void main(String[] args) throws Exception {

		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);


		initInputStateStream(env,fsTableEnv);

		initMachineInfo(env,fsTableEnv);


		// 查询 每小时每个ip一条回缩
		/*Table finalResult = fsTableEnv.sqlQuery(
				"SELECT ip,SUM(cnt) as cSum FROM productDetail GROUP BY ip");*/

		// 查询 来一条追加一条
		/*Table finalResult = fsTableEnv.sqlQuery(
				"SELECT * FROM productDetail");*/


		Table joinResult = fsTableEnv.sqlQuery("SELECT o.*,r.area,r.updateTime FROM productDetail AS o, LATERAL TABLE (Machines(o.mytime)) AS r WHERE r.ip = o.ip");
		joinResult.printSchema();
		fsTableEnv.createTemporaryView("product",joinResult);


		// 查询 每小时每个ip一条追加
		Table productInfo = fsTableEnv.sqlQuery(
				"SELECT ip,area, " +
						"TUMBLE_END(mytime, INTERVAL '30' SECOND) as hourGroup,  " +
						"SUM(cnt) as cSum " +
						"FROM product  " +
						"GROUP BY ip,area,TUMBLE(mytime, INTERVAL '30' SECOND)");
		productInfo.printSchema();
		fsTableEnv.createTemporaryView("productInfo",productInfo);

		//DataStream<Tuple2<Boolean,Row>> stream = fsTableEnv.toRetractStream(productInfo, Row.class);
		DataStream<Row> stream = fsTableEnv.toAppendStream(productInfo, Row.class);

		save2Es(stream);
		// 打印结果
		stream.print();
		env.execute("My Stream");
	}

	private static void save2Es(DataStream<Row> stream){
		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("192.168.1.242", 9200, "http"));

		ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Row>() {
					public IndexRequest createIndexRequest(Row row) {
						Map<String, Object> inputData = new HashMap<>();
						inputData.put("ip", row.getField(0));
						inputData.put("area", row.getField(1));
						inputData.put("hourGroup", ((Timestamp)row.getField(2)).toString());
						inputData.put("cSum", row.getField(3));

						return Requests.indexRequest()
								.index("product_demo")
								.type("demo")
								.source(inputData);
					}

					@Override
					public void process(Row row, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(row));
					}
				}
		);

		esSinkBuilder.setBulkFlushMaxActions(1);

		esSinkBuilder.setRestClientFactory(
				restClientBuilder -> {
				}
		);

		stream.addSink(esSinkBuilder.build());
	}

	// 初始化状态输入流
	private static void initInputStateStream(StreamExecutionEnvironment env,StreamTableEnvironment fsTableEnv){
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.2.213:9092");
		properties.setProperty("group.id", "laozhang");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("state21", new SimpleStringSchema(), properties);
		consumer.setStartFromGroupOffsets();


		DataStream<Product> productStream = env.addSource(consumer).map(data -> JSONObject.parseObject(data,Product.class));
		SingleOutputStreamOperator<Product> productSingleOutputStreamOperator = productStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Product>() {
			long currentMaxTimestamp = 0;

			@Override
			public long extractTimestamp(Product product, long l) {
				long timestamp = product.getLogTime() + 28800000;
				currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
				return timestamp;
			}

			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark( currentMaxTimestamp - 5000);
			}
		});
		fsTableEnv.createTemporaryView("productDetail",productSingleOutputStreamOperator,"ip,cnt,logTime,mytime.proctime");
	}

	// 初始化机台信息变化流
	private static void  initMachineInfo(StreamExecutionEnvironment env,StreamTableEnvironment fsTableEnv){
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.2.213:9092");
		properties.setProperty("group.id", "laozhang");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("mechineinfo", new SimpleStringSchema(), properties);
		consumer.setStartFromGroupOffsets();


		DataStream<MachineInfo> ratesHistoryStream = env.addSource(consumer).map(data -> JSONObject.parseObject(data,MachineInfo.class));

		SingleOutputStreamOperator<MachineInfo> tuple3SingleOutputStreamOperator = ratesHistoryStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<MachineInfo>() {
			long currentMaxTimestamp = 0;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

			@SneakyThrows
			@Override
			public long extractTimestamp(MachineInfo data, long l) {
				long timestamp = sdf.parse(data.getUpdateTime()).getTime() + 28800000;
				currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
				return timestamp;
			}

			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(currentMaxTimestamp);
			}
		});
		fsTableEnv.createTemporaryView("MachineInfo",tuple3SingleOutputStreamOperator, "ip,area,updateTime,curtime.proctime");

		Table machineInfo = fsTableEnv.sqlQuery("select * from MachineInfo");
		machineInfo.printSchema();
		TemporalTableFunction rates = machineInfo.createTemporalTableFunction("curtime", "ip");
		fsTableEnv.registerFunction("Machines", rates);
	}
}
