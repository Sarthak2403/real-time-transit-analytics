import sys
import unittest
import uuid
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))
from simulator import Fleet


class SimulatorTests(unittest.TestCase):
    def test_valid_unique_events_and_routes(self):
        fleet = Fleet(20, 5, seed=7)
        events = [fleet.event() for _ in range(200)]
        self.assertEqual(len({e["event_id"] for e in events}), 200)
        self.assertEqual(len({e["vehicle_id"] for e in events}), 20)
        self.assertEqual(len({e["route_id"] for e in events}), 5)
        for event in events:
            uuid.UUID(event["event_id"])
            self.assertIsNotNone(datetime.fromisoformat(event["timestamp"]).tzinfo)
            self.assertTrue(-90 <= event["latitude"] <= 90)
            self.assertTrue(-180 <= event["longitude"] <= 180)
            self.assertTrue(0 <= event["speed"] <= 160)
        self.assertLess(abs(events[0]["latitude"] - events[20]["latitude"]), 0.001)

    def test_configuration(self):
        with self.assertRaises(ValueError):
            Fleet(0, 1)
        with self.assertRaises(ValueError):
            Fleet(1, 0)


if __name__ == "__main__":
    unittest.main()
