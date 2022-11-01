package com.message.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumerAssig {
	
	public static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);
	
	public static void main(String[] args) {		
		Properties properties = new Properties();
		
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,	"localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "faly-topic");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties); 
		
		try{
			TopicPartition partition = new TopicPartition("faly-topic",2);
			consumer.assign(Arrays.asList(partition));		
			consumer.seek(partition, 44);
			//consumer.subscribe(Arrays.asList("faly-topic"));
			while(true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info(" offset = " + consumerRecord.offset() + ", partition: " + consumerRecord.partition() + ", key: " +
							consumerRecord.key() + ", value: " + consumerRecord.value());
				}					
			}
		}catch (Exception e) {
			log.error("Error de comunicacion");
		}finally {
			consumer.close();
		}
	}
}
