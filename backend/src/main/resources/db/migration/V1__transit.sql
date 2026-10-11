CREATE TABLE transit_events (
    event_id UUID PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL,
    route_id VARCHAR(64) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    speed DOUBLE PRECISION NOT NULL CHECK (speed BETWEEN 0 AND 160),
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX events_vehicle_time_idx ON transit_events (vehicle_id, event_time DESC, event_id DESC);
CREATE INDEX events_route_time_idx ON transit_events (route_id, event_time DESC) INCLUDE (speed, vehicle_id);
CREATE INDEX events_received_idx ON transit_events (received_at);
CREATE TABLE vehicle_locations (
    vehicle_id VARCHAR(64) PRIMARY KEY,
    event_id UUID NOT NULL,
    route_id VARCHAR(64) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    event_time TIMESTAMPTZ NOT NULL
);
CREATE INDEX locations_route_idx ON vehicle_locations (route_id);
