package com.example.httprequestpublish;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.Fuseable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}

@Configuration
class BeanConfiguration{

	@Bean Subscriber<String> idSubscriber() {
		return new Subscriber<String>() {
			private final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class.getName());
			@Override
			public void onSubscribe(Subscription subscription) {
				LOGGER.info("OnSubscribe...");
				subscription.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(String s) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				LOGGER.info("Request-UUID:{} - {}", UUID.randomUUID().toString(),
						String.format("SubscribedTo:%s", s));
			}

			@Override
			public void onError(Throwable throwable) {
				LOGGER.error("Request-UUID:{} - Error", UUID.randomUUID().toString(),
						throwable);
			}

			@Override
			public void onComplete() {
				LOGGER.info("Request-UUID:{} - OnComplete Triggered", UUID.randomUUID().toString());
			}
		};
	}
	@Bean
	public FluxSink<String> getUnicast(Subscriber<String> idSubscriber) {
		UnicastProcessor<String> unicastProcessor = UnicastProcessor.create(new ArrayBlockingQueue<>(1000));
		//unicastProcessor.requestFusion(Fuseable.ASYNC);
		unicastProcessor.publishOn(Schedulers.single()).subscribe(idSubscriber);
		return unicastProcessor.serialize().sink();
	}

/*	@Bean
	public FluxSink<String> getEmitter(Subscriber<String> idSubscriber) {
		EmitterProcessor<String> unicastProcessor = EmitterProcessor.create();
		unicastProcessor.subscribe(idSubscriber);
		return unicastProcessor.sink();
	}*/
}

@RestController
@RequestMapping("/publish/*")
class Controller {
	@Autowired private FluxSink<String> sink;
	private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class.getName());

	@GetMapping("/{id}")
	public ResponseEntity<String> getNext(@PathVariable String id) {
		String requestId = UUID.randomUUID().toString();
		LOGGER.info("Request-UUID:{} Started:{}", requestId, id);

		sink.next(id);
		LOGGER.info("Request-UUID:{} id:{}", requestId, id);
/*		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			LOGGER.error("Request-UUID:{} Concurrency Exception on id:{}", requestId, id, e);
		}*/
		return new ResponseEntity<>(String.format("Hello:%s", id), HttpStatus.OK);
	}
}
