package com.jasim.microservices.twitter.to.kafka.service;


import com.jasim.microservices.config.TwitterToKafkaServiceConfigData;
import com.jasim.microservices.twitter.to.kafka.service.init.StreamInitializer;
import com.jasim.microservices.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.jasim.microservices")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private  static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    private final StreamInitializer streamInitializer;

    private final StreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData configData, StreamInitializer streamInitializer, StreamRunner streamRunner) {
        this.streamInitializer = streamInitializer;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args){
        SpringApplication.run(TwitterToKafkaServiceApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("App Statting....");
        streamInitializer.init();
        streamRunner.start();
    }
}
