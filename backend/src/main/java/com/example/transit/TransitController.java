package com.example.transit;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api")
public class TransitController {
    private final JdbcTemplate jdbc;
    public TransitController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @GetMapping("/vehicles")
    public List<Map<String, Object>> vehicles(@RequestParam(defaultValue = "100") int limit,
                                             @RequestParam(defaultValue = "0") int offset) {
        if (limit < 1 || limit > 1000 || offset < 0) throw badRequest("limit: 1..1000; offset: >=0");
        return jdbc.queryForList("SELECT * FROM vehicle_locations ORDER BY vehicle_id LIMIT ? OFFSET ?", limit, offset);
    }

    @GetMapping("/vehicles/{vehicleId}/location")
    public Map<String, Object> location(@PathVariable String vehicleId) {
        var rows = jdbc.queryForList("SELECT * FROM vehicle_locations WHERE vehicle_id=?", vehicleId);
        if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Vehicle not found");
        return rows.get(0);
    }

    @GetMapping("/routes/{routeId}/statistics")
    public Map<String, Object> routeStatistics(@PathVariable String routeId,
            @RequestParam(required = false) Instant from, @RequestParam(required = false) Instant to) {
        Instant end = to == null ? Instant.now() : to;
        Instant start = from == null ? end.minusSeconds(3600) : from;
        validateWindow(start, end);
        return jdbc.queryForMap("""
            SELECT COUNT(*) AS event_count, COUNT(DISTINCT vehicle_id) AS vehicle_count,
            AVG(speed) AS average_speed, MAX(speed) AS max_speed,
            MIN(event_time) AS first_event_time, MAX(event_time) AS last_event_time
            FROM transit_events WHERE route_id=? AND event_time >= ? AND event_time < ?
            """, routeId, Timestamp.from(start), Timestamp.from(end));
    }

    @GetMapping("/analytics/summary")
    public Map<String, Object> summary() {
        return jdbc.queryForMap("""
            SELECT (SELECT COUNT(*) FROM transit_events) AS total_events,
            COUNT(*) AS total_vehicles, COUNT(DISTINCT route_id) AS total_routes,
            AVG(speed) AS average_latest_speed,
            COUNT(*) FILTER (WHERE event_time >= now() - interval '5 minutes') AS active_vehicles,
            MAX(event_time) AS latest_event_time FROM vehicle_locations
            """);
    }

    private void validateWindow(Instant start, Instant end) {
        if (!start.isBefore(end) || start.isBefore(end.minusSeconds(86400))) {
            throw badRequest("Window must be positive and at most 24 hours");
        }
    }
    private ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
