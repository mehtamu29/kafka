package com.jpmc.nkb.kafka.consumer;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class KafkaConsumerApplication {

	public static void main(String[] args) {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(KafkaConfiguration.class);
		ctx.refresh();
	}
}