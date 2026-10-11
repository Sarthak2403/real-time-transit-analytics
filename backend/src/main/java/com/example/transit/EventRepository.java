package com.example.transit;

import java.sql.Timestamp;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class EventRepository {
    private final JdbcTemplate jdbc;
    public EventRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional
    public void save(TransitEvent e) {
        int inserted = jdbc.update("""
            INSERT INTO transit_events(event_id,vehicle_id,route_id,latitude,longitude,speed,event_time)
            VALUES (?,?,?,?,?,?,?) ON CONFLICT (event_id) DO NOTHING
            """, e.eventId(), e.vehicleId(), e.routeId(), e.latitude(), e.longitude(), e.speed(), Timestamp.from(e.timestamp()));
        if (inserted == 0) return;
        jdbc.update("""
            INSERT INTO vehicle_locations(vehicle_id,event_id,route_id,latitude,longitude,speed,event_time)
            VALUES (?,?,?,?,?,?,?) ON CONFLICT (vehicle_id) DO UPDATE SET
            event_id=EXCLUDED.event_id,route_id=EXCLUDED.route_id,latitude=EXCLUDED.latitude,
            longitude=EXCLUDED.longitude,speed=EXCLUDED.speed,event_time=EXCLUDED.event_time
            WHERE (EXCLUDED.event_time,EXCLUDED.event_id) > (vehicle_locations.event_time,vehicle_locations.event_id)
            """, e.vehicleId(), e.eventId(), e.routeId(), e.latitude(), e.longitude(), e.speed(), Timestamp.from(e.timestamp()));
    }
}
