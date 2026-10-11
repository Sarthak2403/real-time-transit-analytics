package com.example.transit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Validator;
import org.springframework.stereotype.Service;

@Service
public class EventProcessor {
    private final ObjectMapper mapper;
    private final Validator validator;
    private final EventRepository repository;

    public EventProcessor(ObjectMapper mapper, Validator validator, EventRepository repository) {
        this.mapper = mapper;
        this.validator = validator;
        this.repository = repository;
    }

    public void process(String payload, String key) {
        TransitEvent event;
        try {
            event = mapper.readValue(payload, TransitEvent.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new InvalidEventException("Malformed event", e);
        }
        if (event == null || !validator.validate(event).isEmpty()) {
            throw new InvalidEventException("Event failed validation");
        }
        if (!Double.isFinite(event.latitude()) || !Double.isFinite(event.longitude())
                || !Double.isFinite(event.speed()) || !event.vehicleId().equals(key)) {
            throw new InvalidEventException("Invalid coordinates, speed or vehicle key");
        }
        repository.save(event);
    }
}
