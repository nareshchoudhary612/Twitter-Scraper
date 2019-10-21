package com.twitter.kafka.service;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.kafka.producer.TwitterProducer;
import com.twitter.kafka.service.utils.KafkaProducerFactory;

@Service
public class TwitterToKafkaService implements Runnable {
	@Value("${twitter.consumerkey}")
	String consumerKey;

	@Value("${twitter.consumerSecret}")
	String consumerSecret;

	@Value("${twitter.token}")
	String token;

	@Value("${twitter.secret}")
	String secret;

	List<String> terms = Lists.newArrayList("kafka","USA");
	
	@Autowired
	KafkaProducerFactory kafkaProducerFactory;

	
	public void run() {

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		// Attemp to connect
		client.connect();

		// create a kafka producer
		//KafkaProducer<String, String> producer = createKafkaProducer();
		KafkaProducer<String, String> producer = kafkaProducerFactory.getBasicProducer();

		// adding a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("stopping app & shutting down twitter client");
			client.stop();
			System.out.println("closing producer");
			producer.close();
			System.out.println("done");

		}));

		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				System.out.println(msg);

				producer.send(new ProducerRecord<String, String>("second_topic", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						// TODO Auto-generated method stub
						if (e != null) {
							System.out.println("something bad happend " + e);
						}
					}
				});
			}
		}
		System.out.println("end of app");

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);

		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		// .eventMessageQueue(eventQueue); // optional: use this if you want to process
		// client events

		Client hosebirdClient = builder.build();
		return hosebirdClient;
		// Attempts to establish a connection.
		// hosebirdClient.connect();
	}

	
}
