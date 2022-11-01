package com.message.kafka.producer.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.message.kafka.transactional.TransactionalProducer;

public class CallBacks implements Callback{

	public static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception != null) {
			log.info("Error en envíos del producer: " + exception.getMessage());
		}
		log.info("offset: " + metadata.offset() + ", topic: "+ metadata.topic() + ", partition: " + metadata.partition() );
	}
	

}
