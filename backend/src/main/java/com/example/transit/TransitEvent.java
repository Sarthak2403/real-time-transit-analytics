package com.example.transit;

import java.time.Instant;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.*;

public record TransitEvent(
    @JsonProperty("event_id") @NotNull UUID eventId,
    @JsonProperty("vehicle_id") @NotBlank @Size(max = 64) String vehicleId,
    @JsonProperty("route_id") @NotBlank @Size(max = 64) String routeId,
    @NotNull @DecimalMin("-90") @DecimalMax("90") Double latitude,
    @NotNull @DecimalMin("-180") @DecimalMax("180") Double longitude,
    @NotNull @DecimalMin("0") @DecimalMax("160") Double speed,
    @NotNull Instant timestamp
) {}
