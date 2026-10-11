package com.example.transit;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.apache.kafka.clients.admin.NewTopic;

@Configuration
public class KafkaConfig {
    @Bean NewTopic events(@Value("${transit.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(6).replicas(1).build();
    }
    @Bean NewTopic dlq(@Value("${transit.dlq}") String topic) {
        return TopicBuilder.name(topic).partitions(6).replicas(1).build();
    }
    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
                                          @Value("${transit.dlq}") String dlq) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
            (record, exception) -> new TopicPartition(dlq, record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        var backoff = new ExponentialBackOffWithMaxRetries(5);
        backoff.setInitialInterval(1000);
        backoff.setMultiplier(2);
        backoff.setMaxInterval(10000);
        var handler = new DefaultErrorHandler(recoverer, backoff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setCommitRecovered(true);
        return handler;
    }
}
