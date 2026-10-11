package com.example.transit;

import java.time.Instant;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.validation.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EventProcessorTest {
    private final EventRepository repository = mock(EventRepository.class);
    private final ObjectMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private EventProcessor processor;
    @BeforeEach void setup() {
        processor = new EventProcessor(mapper, Validation.buildDefaultValidatorFactory().getValidator(), repository);
    }
    private TransitEvent event(Double latitude, Double speed) {
        return new TransitEvent(UUID.randomUUID(), "v1", "r1", latitude, -87.6, speed, Instant.now());
    }
    @Test void persistsValidEvent() throws Exception {
        var e = event(41.8, 35.0);
        processor.process(mapper.writeValueAsString(e), "v1");
        verify(repository).save(e);
    }
    @Test void rejectsInvalidFields() throws Exception {
        for (var e : new TransitEvent[]{event(91.0, 10.0), event(null, 10.0), event(41.8, -1.0)}) {
            assertThrows(InvalidEventException.class, () -> processor.process(mapper.writeValueAsString(e), "v1"));
        }
        verifyNoInteractions(repository);
    }
    @Test void rejectsMalformedAndWrongKey() throws Exception {
        assertThrows(InvalidEventException.class, () -> processor.process("broken", "v1"));
        assertThrows(InvalidEventException.class, () -> processor.process("null", "v1"));
        assertThrows(InvalidEventException.class, () -> processor.process(mapper.writeValueAsString(event(41.8, 2.0)), "v2"));
        verifyNoInteractions(repository);
    }
}
