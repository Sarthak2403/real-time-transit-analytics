"""Export committed, closed receive-time hours without advancing past failed uploads."""
import fcntl
import gzip
import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

UTC = timezone.utc
HOUR = timedelta(hours=1)


def parse_hour(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError('Export timestamps must include UTC timezone')
    if result.minute or result.second or result.microsecond:
        raise ValueError('Export timestamps must be aligned to an hour')
    return result


def object_key(start):
    return start.strftime('telemetry/received_date=%Y-%m-%d/hour=%H/events.jsonl.gz')


def json_value(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def write_gzip(rows, path):
    count = 0
    with open(path, 'wb') as raw:
        with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as compressed:
            for row in rows:
                compressed.write((json.dumps(row, default=json_value, allow_nan=False,
                                             separators=(',', ':')) + '\n').encode())
                count += 1
    return count


def retry(operation, attempts=4, sleep=time.sleep):
    for attempt in range(attempts):
        try:
            return operation()
        except Exception:
            if attempt + 1 == attempts:
                raise
            sleep(min(2 ** attempt, 8))


def save_checkpoint(path, next_hour, bucket, start):
    payload = dict(next_hour=next_hour.isoformat(), bucket=bucket, export_start=start.isoformat())
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix='.checkpoint-')
    try:
        with os.fdopen(descriptor, 'w') as output:
            json.dump(payload, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def export_windows(reader, upload, checkpoint, start, cutoff, bucket):
    current = start
    if checkpoint.exists():
        saved = json.loads(checkpoint.read_text())
        if saved['bucket'] != bucket or saved['export_start'] != start.isoformat():
            raise ValueError('Checkpoint configuration changed; use a separate state directory')
        current = parse_hour(saved['next_hour'])
        if current < start:
            raise ValueError('Checkpoint predates export start')
    while current + HOUR <= cutoff:
        end = current + HOUR
        with tempfile.TemporaryDirectory(dir=checkpoint.parent) as temporary:
            path = Path(temporary) / 'events.jsonl.gz'
            count = reader(current, end, path)
            if count:
                retry(lambda: upload(path, object_key(current)))
            # Empty windows are safe to checkpoint because the reader fences writers.
            save_checkpoint(checkpoint, end, bucket, start)
            logging.info('Archived %s events for %s', count, current.isoformat())
        current = end
    return current


def read_window(connect, start, end, path):
    # Fence in-flight inserts, acquire a snapshot, then immediately release the fence.
    # Copying/uploading does not hold the ingestion-blocking table lock.
    from psycopg.rows import dict_row
    with connect() as guard, connect() as snapshot:
        guard.execute("SET LOCAL lock_timeout = '5s'")
        guard.execute('LOCK TABLE transit_events IN SHARE MODE')
        snapshot.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        snapshot.execute('SELECT pg_current_snapshot()').fetchone()
        database_now = guard.execute('SELECT clock_timestamp()').fetchone()[0].astimezone(UTC)
        if end > database_now.replace(minute=0, second=0, microsecond=0):
            raise ValueError('Refusing to export an open receive-time hour')
        guard.commit()
        with snapshot.cursor(name='telemetry_export', row_factory=dict_row) as cursor:
            cursor.itersize = 5000
            cursor.execute('''SELECT event_id,vehicle_id,route_id,latitude,longitude,speed,
                              event_time AS timestamp,received_at FROM transit_events
                              WHERE received_at >= %s AND received_at < %s
                              ORDER BY received_at,event_id''', (start, end))
            return write_gzip(cursor, path)


def main():
    import boto3
    import psycopg
    from botocore.config import Config
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    bucket = os.environ['S3_BUCKET']
    start = parse_hour(os.environ['EXPORT_START'])
    state = Path(os.getenv('EXPORT_STATE_DIR', '/var/lib/transit-export'))
    state.mkdir(parents=True, exist_ok=True)
    with (state / 'export.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        options = dict(host=os.getenv('POSTGRES_HOST', 'localhost'),
                       port=os.getenv('POSTGRES_PORT', '5432'),
                       dbname=os.getenv('POSTGRES_DB', 'transit'),
                       user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'],
                       connect_timeout=10, options='-c timezone=UTC')
        connect = lambda: psycopg.connect(**options)
        with connect() as connection:
            cutoff = connection.execute("SELECT date_trunc('hour',clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'").fetchone()[0]
        s3 = boto3.client('s3', region_name=os.environ['AWS_REGION'],
                          config=Config(retries={'mode': 'standard', 'total_max_attempts': 4}))
        def upload(path, key):
            s3.upload_file(str(path), bucket, key, ExtraArgs={
                'ServerSideEncryption': 'AES256', 'ContentType': 'application/x-ndjson',
                'ContentEncoding': 'gzip'})
        export_windows(lambda a, b, p: retry(lambda: read_window(connect, a, b, p)),
                       upload, state / 'checkpoint.json', start, cutoff, bucket)


if __name__ == '__main__':
    main()
