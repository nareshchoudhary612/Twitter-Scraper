package com.twitter.kafka.producer;

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
import org.hibernate.validator.internal.util.logging.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;

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

public class TwitterProducer {

	// Logger logger = LoggerFactory. getLogger(TwitterProducer.class.getName());

	String consumerKey = "OFDh9B4pMOArTZZHvXm97ePyx";
	String consumerSecret = "FE9UPcVSybRfOh9PyQwmJ9q9lLaYLMrdhJgzo1JyqcHWm5mZbt";
	String token = "1140790771525836800-lxReHg2cGIQd1wWdiZIrus8vfqUcX6";
	String secret = "1KkTodSCA5dhsI4AbpIPakhTdZQyuDQsNdRYmiLjjeIgO";

	List<String> terms = Lists.newArrayList("kafka", "trump", "usa", "india");

	public TwitterProducer() {
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

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
		KafkaProducer<String, String> producer = createKafkaProducer();

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

	public KafkaProducer<String, String> createKafkaProducer() {

		String bootstrapServers = "127.0.0.1:9092";

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // 1 for kafka <1.1

		// high throughput producer
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32kb

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}
}
