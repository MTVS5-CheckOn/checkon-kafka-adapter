package com.checkon.aiadapter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan
@SpringBootApplication
public class CheckonKafkaAdapterApplication {

	public static void main(String[] args) {
		SpringApplication.run(CheckonKafkaAdapterApplication.class, args);
	}
}
