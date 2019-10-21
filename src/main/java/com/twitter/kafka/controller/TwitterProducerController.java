package com.twitter.kafka.controller;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.twitter.kafka.service.TwitterToKafkaService;

@RestController
public class TwitterProducerController {

	@Autowired
	TwitterToKafkaService twitterToKafkaService;
	
	ExecutorService executorService = Executors.newFixedThreadPool(1);
	
	@GetMapping("/startProducerInBackground")
	public ResponseEntity startConsumerInBackground() {
		executorService.execute(twitterToKafkaService);
		return new ResponseEntity<>(HttpStatus.ACCEPTED);
	}

	/* Stopping the background running service */
	@GetMapping("/stopProducerInBackground")
	public ResponseEntity stopProducerInBackground() {
		executorService.shutdownNow();
		return new ResponseEntity<>(HttpStatus.ACCEPTED);
	}
}
