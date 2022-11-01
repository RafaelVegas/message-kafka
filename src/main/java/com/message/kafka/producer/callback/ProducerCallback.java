package com.message.kafka.producer.callback;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.message.kafka.transactional.TransactionalProducer;

public class ProducerCallback {
	
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
			for (int i = 1; i <= 100; i++) {
				producer.send(new ProducerRecord<String, String>("faly-primer-topic", (i%2 == 0) ? "key-2.1" : "key-3.1" ,
						 "value of message " + i),new CallBacks());
			}
			producer.flush();
		} catch (Exception ex) {
			log.error("Error en el envío del producer");
		}finally {
			producer.close();
		}
	}
}
