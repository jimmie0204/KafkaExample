package com.jasongj.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import com.jasongj.kafka.util.KafkaConstant;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DemoConsumerAssign {

	public static void main(String[] args) {
		args = new String[] {KafkaConstant.KAFKA_SERVER, "topic_jimmie", "group_jimmie2", "consumer1" };
		if (args == null || args.length != 4) {
			System.err.println(
					"Usage:\n\tjava -jar kafka_consumer.jar ${bootstrap_server} ${topic_name} ${group_name} ${client_id}");
			System.exit(1);
		}
		String bootstrap = args[0];
		String topic = args[1];
		String groupid = args[2];
		String clientid = args[3];

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrap);
		props.put("group.id", groupid);
		props.put("client.id", clientid);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("auto.offset.reset", "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.assign(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
		while (true) {
			System.out.println("开始拉取一波====");
//			consumer.seek(new TopicPartition(topic, 0),1);
			ConsumerRecords<String, String> records = consumer.poll(1000);
			records.forEach(record -> {
				System.out.printf("开始打印==========client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s%n", clientid, record.topic(),
						record.partition(), record.offset(), record.key(), record.value());
			});
		}
	}

}
