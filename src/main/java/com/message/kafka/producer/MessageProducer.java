package com.message.kafka.producer;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer {
	
	public static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

	public static void main(String[] args) {
		Properties properties = new Properties();
		
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		try{ 
			for (int i = 0; i <= 10000000; i++) {
				producer.send(new ProducerRecord<String, String>("faly-primer-topic", String.valueOf(i), "value of message"));
			}
			producer.flush();
		} catch (Exception ex) {
			log.error("Error en el envío del producer");
		}finally {
			producer.close();
		}
		
		
		
	}
}
