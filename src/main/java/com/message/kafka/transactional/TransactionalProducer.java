package com.message.kafka.transactional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProducer {
	
	public static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

	public static void main(String[] args) {		
		Properties properties = new Properties();
		
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("transactional.id", "faly-primer-topic-id");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("linger.ms", "10");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		try{ 
			producer.initTransactions();
			producer.beginTransaction();
			for (int i = 1; i <= 10000000; i++) {
				producer.send(new ProducerRecord<String, String>("faly-primer-topic", String.valueOf(i), "value of message"));
			}
			producer.commitTransaction();
			producer.flush();
		} catch (Exception ex) {
			log.error("Error en el envío del producer");
		}finally {
			producer.close();
		}
	}
}
