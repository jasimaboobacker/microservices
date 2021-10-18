package com.jasim.microservices.twitter.to.kafka.service.runner.impl;

import com.jasim.microservices.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.jasim.microservices.twitter.to.kafka.service.listener.TwitterStatusKafkaListener;
import com.jasim.microservices.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.lang.reflect.Array;
import java.util.Arrays;

@Component
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterStatusKafkaListener twitterStatusKafkaListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData configData, TwitterStatusKafkaListener statusListener) {
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterStatusKafkaListener = statusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterStatusKafkaListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown(){
        if(twitterStream != null) {
            LOG.info("Closing twitter stream");
            twitterStream.shutdown();
        }
    }

    private void addFilter(){
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("started filtering for keywords {}", Arrays.toString(keywords));
    }


}
