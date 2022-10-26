package com.message.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessageProducer {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> produceeeer = new KafkaProducer<>(properties);
		produceeeer.send(new ProducerRecord<String, String>("faly-primer-topic", "key-message", "value of message"));
		produceeeer.close();
		
		
	}
}
