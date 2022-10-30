package com.message.kafka.multithread;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadConsumer extends Thread {
	
	public static final Logger log = LoggerFactory.getLogger(ThreadConsumer.class);
	
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private final KafkaConsumer<String, String> consumer;

	public ThreadConsumer(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}	

	@Override
	public void run() {
		consumer.subscribe(Arrays.asList("faly-primer-topic"));	
		try {
			while(!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.debug(" offset = " + consumerRecord.offset() + ", partition: " + consumerRecord.partition() + ", key: " +
							consumerRecord.key() + ", value:" + consumerRecord.value() + consumerRecord.toString());
					
					if( Integer.parseInt(consumerRecord.key()) % 1000000 == 0) {
						log.info(" offset = " + consumerRecord.offset() + ", partition: " + consumerRecord.partition() + ", key: " +
								consumerRecord.key() + ", value:" + consumerRecord.value() + consumerRecord.toString());
					}
				}
			}
		}catch (WakeupException e) {
			if(!closed.get()) throw e;
		}finally {
			consumer.close();
		}		
	}
	
	public void shutdown() {
		consumer.wakeup();
	}

}
