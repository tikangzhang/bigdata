package producer.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GenericProducer {
	private static final SimpleDateFormat stringToDate= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	protected Producer<String, String> procuder;

    public GenericProducer(String servers){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("compression.type", "snappy");
        this.procuder = new KafkaProducer<String, String>(props);
    }

    protected void send(String topic,String record){
        this.procuder.send(new ProducerRecord<String, String>(topic,record));
    }

    protected void close(){
        if(this.procuder != null){
            this.procuder.close(100, TimeUnit.MILLISECONDS);
        }
    }
}
