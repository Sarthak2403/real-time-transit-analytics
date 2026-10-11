package com.example.transit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {
    private final EventProcessor processor;
    public EventConsumer(EventProcessor processor) { this.processor = processor; }

    @KafkaListener(topics = "${transit.topic}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        processor.process(record.value(), record.key());
        acknowledgment.acknowledge();
    }
}
