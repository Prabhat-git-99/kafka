package com.conducktor.demos.kafka_step;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemoKeys {
	
	private static final Logger log = LoggerFactory.getLogger( ProducerDemoKeys.class.getSimpleName( ) );
	

	public static void main(String[] args) { 
		// TODO Auto-generated method stub
		
		log.info( "Hello world!!!" );
		
		//****************************
		// Simple Program 
		// to send data to kafka
		//****************************
		
		// Create Producer Properties
		Properties properties = new Properties( );
		properties.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092" );
		// properties.setProperty( "bootstrap.servers", "127.0.0.1:9092" );
		properties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName( ) );
		properties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName( ) );
		
		
		// Create the Producer
		KafkaProducer< String, String > producer = new KafkaProducer< String, String >( properties );
		
		
		for ( int i=0; i<10; i++ ) {
			
			String topic = "demo_java";
			String key = "id_" + i;
			String value = "hello world" + i;
			
			
			// Create a producer record
			ProducerRecord< String, String > producerRecord = 
					new ProducerRecord< String, String >( topic, key, value );
			
			
			// Send data -> It's an asynchronous opertation
			producer.send( producerRecord, new Callback( ) { 
				@Override
				public void onCompletion( RecordMetadata metadata, Exception e ) {
					// Executes every time a record is successfully sent or an exception is thrown
					
					if ( e == null ) {
						log.info( "Received new metadata/ \n" + "Topic: " + metadata.topic( ) + "\n" + 
								"Key: " + producerRecord.key( ) + "\n" +
								"Partition: " + metadata.partition( ) + "\n" +
								"Offset: " + metadata.offset( ) + "\n" +
								"Timestamp: " + metadata.timestamp( ) 
						);
					}
					else {
						
						log.error( "Error while producing: ", e );
						
					}
					
				}
			} );
			
			
			
		}
		
		// Flush & Close the Producer - Synchronous data
		producer.flush( );
		
		producer.close( );
		
		
	}

}
