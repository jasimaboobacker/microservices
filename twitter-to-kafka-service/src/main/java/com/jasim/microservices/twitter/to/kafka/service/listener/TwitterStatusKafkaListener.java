package com.jasim.microservices.twitter.to.kafka.service.listener;

import com.jasim.microservices.config.KafkaConfigData;
import com.jasim.microservices.kafka.avro.model.TwitterAvroModel;
import com.jasim.microservices.kafka.producer.config.service.KafkaProducer;
import com.jasim.microservices.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterStatusKafkaListener extends StatusAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterStatusKafkaListener.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

    public TwitterStatusKafkaListener(KafkaConfigData kafkaConfigData, KafkaProducer<Long, TwitterAvroModel> kafkaProducer, TwitterStatusToAvroTransformer twitterStatusToAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatusToAvroTransformer = twitterStatusToAvroTransformer;
    }

    @Override
    public void onStatus(Status status) {
        LOG.info("Recieved status with text {} sending to kafka topic {}", status.getText(),kafkaConfigData.getTopicName());
        TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer.getTwitterAvroModel(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
        //super.onStatus(status);
    }
}
