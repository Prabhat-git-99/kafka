package com.conducktor.demos.kafka_step;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	private static final Logger log = LoggerFactory.getLogger( ProducerDemo.class.getSimpleName( ) );

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
				
		log.info( "I am kafka consumer" );
		
		Properties properties = new Properties( );
		
		String bootstrapServer = "127.0.0.1:9092";
		String groupId = "my-second-application";
		String topics = "demo_java";
		
		// Create consumer config
		properties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer );
		properties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName( ) );
		properties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName( ) );
		properties.setProperty( ConsumerConfig.GROUP_ID_CONFIG, groupId );
		properties.setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );
		
		// Create consumer
		KafkaConsumer< String, String > consumer = new KafkaConsumer< String, String >( properties );
		
		// Subscribe consumr to our topics
		// consumer.subscribe( Collections.singletonList( topics ) );
		consumer.subscribe( Arrays.asList( topics ) );
		
		// Poll for new data
		while( true ) {
			
			log.info( "Polling..." );
			
			ConsumerRecords< String, String > records = consumer.poll( Duration.ofMillis( 100 ) );
			
			for ( ConsumerRecord< String, String > record: records ) {
				
				log.info( "Key: " + record.key( ) + " , Value: " + record.value( ) );
				log.info( "Partition: " + record.partition( ) + " , Offset: " + record.offset( ) );
				
			}
			
		}
		
		
	}

}
