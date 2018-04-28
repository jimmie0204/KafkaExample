package com.jasongj.kafka.producer;

import java.util.Properties;
import java.util.concurrent.Future;

import com.jasongj.kafka.util.KafkaConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConstant.KAFKA_SERVER);
		props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("linger.ms", 0);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		props.put("partitioner.class", HashPartitioner.class.getName());
		props.put("interceptor.classes", EvenProducerInterceptor.class.getName());

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		for (int i = 0; i < 20; i++){
			Future<RecordMetadata> topic1 = producer.send(new ProducerRecord<String, String>("topic_jimmie", Integer.toString(i), Integer.toString(i)));
//			RecordMetadata recordMetadata = topic1.get();
//			System.out.println(recordMetadata);

		}
		System.out.println("==done");
		producer.close();

	}

}
