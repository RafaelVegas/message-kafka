package com.message.kafka.multithread;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MultiTheadConsumer {
	public static void main(String[] args) {
		Properties properties = new Properties();
		
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,	"localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "faly-primer-topic");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE);
		//properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());	
		
		ExecutorService executor = Executors.newFixedThreadPool(4);
		
		for (int i = 1; i <=4 ; i++) {
			ThreadConsumer consumer = new ThreadConsumer( new KafkaConsumer<String, String>(properties) );
			executor.execute(consumer);
		}
		while(!executor.isTerminated()) {}
	}
}
