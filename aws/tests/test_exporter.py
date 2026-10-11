import gzip
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from export_telemetry import export_windows, object_key, parse_hour, retry, write_gzip, HOUR


class ExporterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.checkpoint = Path(self.temporary.name) / 'checkpoint.json'
        self.start = parse_hour('2026-10-10T23:00:00Z')

    def reader(self, start, end, path):
        return write_gzip([{'event_id': 'event-1', 'timestamp': start, 'speed': 30}], path)

    def test_deterministic_key_and_gzip(self):
        self.assertEqual(object_key(self.start), 'telemetry/received_date=2026-10-10/hour=23/events.jsonl.gz')
        paths = [Path(self.temporary.name) / name for name in ('one.gz', 'two.gz')]
        for path in paths:
            self.reader(self.start, self.start + HOUR, path)
        self.assertEqual(paths[0].read_bytes(), paths[1].read_bytes())
        self.assertEqual(json.loads(gzip.decompress(paths[0].read_bytes()))['timestamp'], self.start.isoformat())

    def test_success_resume_and_empty_window(self):
        upload = Mock()
        export_windows(self.reader, upload, self.checkpoint, self.start, self.start + 2*HOUR, 'bucket')
        self.assertEqual(upload.call_count, 2)
        self.assertEqual(json.loads(self.checkpoint.read_text())['next_hour'], (self.start+2*HOUR).isoformat())
        export_windows(self.reader, upload, self.checkpoint, self.start, self.start + 2*HOUR, 'bucket')
        self.assertEqual(upload.call_count, 2)
        export_windows(lambda a,b,p: write_gzip([],p), upload, self.checkpoint, self.start, self.start+3*HOUR, 'bucket')
        self.assertEqual(upload.call_count, 2)
        self.assertEqual(json.loads(self.checkpoint.read_text())['next_hour'], (self.start+3*HOUR).isoformat())

    @patch('export_telemetry.retry', side_effect=lambda operation: retry(operation, sleep=lambda _:None))
    def test_failed_upload_does_not_advance_and_retry_uses_same_key(self, _):
        upload = Mock(side_effect=OSError('network'))
        with self.assertRaises(OSError):
            export_windows(self.reader, upload, self.checkpoint, self.start, self.start+HOUR, 'bucket')
        self.assertFalse(self.checkpoint.exists())
        self.assertEqual(upload.call_count, 4)
        self.assertEqual({call.args[1] for call in upload.call_args_list}, {object_key(self.start)})
        upload.side_effect = None
        export_windows(self.reader, upload, self.checkpoint, self.start, self.start+HOUR, 'bucket')
        self.assertTrue(self.checkpoint.exists())

    def test_failed_read_and_checkpoint_configuration(self):
        with self.assertRaises(OSError):
            export_windows(Mock(side_effect=OSError()), Mock(), self.checkpoint, self.start,self.start+HOUR,'bucket')
        self.assertFalse(self.checkpoint.exists())
        export_windows(self.reader, Mock(), self.checkpoint, self.start,self.start+HOUR,'bucket')
        with self.assertRaises(ValueError):
            export_windows(self.reader, Mock(), self.checkpoint, self.start,self.start+HOUR,'other-bucket')

    def test_open_hour_not_exported(self):
        upload = Mock()
        export_windows(self.reader, upload, self.checkpoint, self.start, self.start+HOUR/2, 'bucket')
        upload.assert_not_called()
        self.assertFalse(self.checkpoint.exists())

    def test_atomic_checkpoint_failure_replays_confirmed_upload(self):
        upload = Mock()
        with patch('export_telemetry.os.replace', side_effect=OSError('disk')):
            with self.assertRaises(OSError):
                export_windows(self.reader, upload, self.checkpoint,self.start,self.start+HOUR,'bucket')
        self.assertFalse(self.checkpoint.exists())
        export_windows(self.reader, upload, self.checkpoint,self.start,self.start+HOUR,'bucket')
        self.assertEqual(upload.call_count,2)
        self.assertEqual(upload.call_args_list[0].args[1],upload.call_args_list[1].args[1])

    def test_retry_and_timestamp_validation(self):
        operation = Mock(side_effect=[OSError(), 'ok'])
        sleep = Mock()
        self.assertEqual(retry(operation, sleep=sleep), 'ok')
        sleep.assert_called_once_with(1)
        for value in ('2026-10-10T23:00:00', '2026-10-10T23:05:00Z', '2026-10-10T23:00:00-05:00'):
            with self.assertRaises(ValueError):
                parse_hour(value)


if __name__ == '__main__':
    unittest.main()
