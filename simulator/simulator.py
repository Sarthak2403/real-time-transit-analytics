import argparse
import json
import math
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


class Fleet:
    def __init__(self, vehicles=100, routes=10, seed=None):
        if vehicles < 1 or routes < 1:
            raise ValueError("vehicles and routes must be positive")
        self.random = random.Random(seed)
        self.routes = routes
        self.positions = [self.random.random() * 2 * math.pi for _ in range(vehicles)]
        self.last = [None] * vehicles
        self.index = 0

    def event(self):
        vehicle = self.index % len(self.positions)
        self.index += 1
        route = vehicle % self.routes
        now = time.monotonic()
        elapsed = 0 if self.last[vehicle] is None else min(now - self.last[vehicle], 30)
        self.last[vehicle] = now
        angle = self.positions[vehicle]
        speed = max(0, 32 + 18 * math.sin(angle * 3) + self.random.gauss(0, 2))
        # Circular urban routes, roughly 1.3 km radius, with smooth GPS movement.
        self.positions[vehicle] += speed / 3.6 * elapsed / 1300
        angle = self.positions[vehicle]
        return {
            "event_id": str(uuid.uuid4()), "vehicle_id": f"vehicle-{vehicle:04d}",
            "route_id": f"route-{route:03d}",
            "latitude": round(41.88 + route * 0.002 + 0.0117 * math.sin(angle), 6),
            "longitude": round(-87.63 + route * 0.002 + 0.0157 * math.cos(angle), 6),
            "speed": round(speed, 2), "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def main():
    from confluent_kafka import Producer
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=float(os.getenv("EVENTS_PER_SECOND", "250")))
    parser.add_argument("--vehicles", type=int, default=int(os.getenv("VEHICLE_COUNT", "100")))
    parser.add_argument("--routes", type=int, default=int(os.getenv("ROUTE_COUNT", "10")))
    parser.add_argument("--duration", type=float, default=0)
    args = parser.parse_args()
    if not math.isfinite(args.rate) or not math.isfinite(args.duration) or args.rate <= 0 or args.duration < 0:
        parser.error("rate must be positive; duration must be nonnegative")
    fleet = Fleet(args.vehicles, args.routes)
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "enable.idempotence": True, "acks": "all", "compression.type": "lz4",
        "linger.ms": 10, "delivery.timeout.ms": 120000,
    })
    running = True
    delivered = 0
    failed = 0

    def stop(*_):
        nonlocal running
        running = False

    def delivery(error, _message):
        nonlocal delivered, failed
        if error:
            failed += 1
            print(f"Delivery failed: {error}", flush=True)
        else:
            delivered += 1

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    topic = os.getenv("KAFKA_TOPIC", "transit-events")
    started = time.monotonic()
    sent = 0
    next_emit = started
    while running and (args.duration == 0 or time.monotonic() - started < args.duration):
        event = fleet.event()
        while running:
            try:
                producer.produce(topic, key=event["vehicle_id"], value=json.dumps(event), on_delivery=delivery)
                sent += 1
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        Path("/tmp/simulator-heartbeat").touch()
        next_emit = max(next_emit + 1 / args.rate, time.monotonic())
        delay = next_emit - time.monotonic()
        while running and delay > 0:
            if args.duration and time.monotonic() - started >= args.duration:
                break
            Path("/tmp/simulator-heartbeat").touch()
            time.sleep(min(delay, 1))
            delay = next_emit - time.monotonic()
    pending = producer.flush(30)
    elapsed = time.monotonic() - started
    print(json.dumps({"sent": sent, "delivered": delivered, "failed": failed,
                      "pending": pending, "elapsed_seconds": elapsed,
                      "delivered_per_second": delivered / elapsed}))
    if failed or pending:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
