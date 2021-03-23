package com.jpmc.training.kafkareceiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleReceiverTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Properties props=new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
		
		KafkaConsumer<String, String> consumer=new KafkaConsumer<>(props);
		
		List<String> topics=new ArrayList();
		topics.add("test-topic-v");
		
		consumer.subscribe(topics);
		System.out.println("waiting for messages");
		while(true) {
			ConsumerRecords<String, String>  records=consumer.poll(Duration.ofSeconds(20));
			records.forEach(record->System.out.println("key: "+record.key()+"\tvalue: "+record.value()+" from partition: "+record.partition()));
		}
		
	}

}
