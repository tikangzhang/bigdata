package com.laozhang.common;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaStreamBuilder {
    private Properties properties = new Properties();
    private String topic;
    public KafkaStreamBuilder(String servers,String groupId){
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);

    }

    public KafkaStreamBuilder topic(String topic){
        this.topic = topic;
        return this;
    }

    public FlinkKafkaConsumer<String> build(){
        if(this.topic != null) {
            FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
            flinkKafkaConsumer.setStartFromGroupOffsets();
            return flinkKafkaConsumer;
        }else{
            throw new RuntimeException("Topic never set!");
        }
    }
}
