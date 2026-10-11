package com.example.transit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventConsumerTest {
    @Test void acknowledgesOnlyAfterPersistence() {
        var processor = mock(EventProcessor.class);
        var ack = mock(Acknowledgment.class);
        new EventConsumer(processor).consume(new ConsumerRecord<>("events", 0, 1, "v1", "payload"), ack);
        var order = inOrder(processor, ack);
        order.verify(processor).process("payload", "v1");
        order.verify(ack).acknowledge();
    }
    @Test void doesNotAcknowledgeFailureAndCanRetry() {
        var processor = mock(EventProcessor.class);
        var ack = mock(Acknowledgment.class);
        var consumer = new EventConsumer(processor);
        var record = new ConsumerRecord<String, String>("events", 0, 1, "v1", "payload");
        doThrow(new RuntimeException("database unavailable")).doNothing().when(processor).process("payload", "v1");
        assertThrows(RuntimeException.class, () -> consumer.consume(record, ack));
        verifyNoInteractions(ack);
        consumer.consume(record, ack);
        verify(ack).acknowledge();
    }
}
