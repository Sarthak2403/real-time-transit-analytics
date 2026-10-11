"""Optional Docker-only test: isolated database, no AWS calls or local pipeline changes."""
import gzip
import json
import os
import secrets
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from export_telemetry import read_window, parse_hour, HOUR, write_gzip


@unittest.skipUnless(os.getenv('RUN_POSTGRES_TESTS')=='1','Set RUN_POSTGRES_TESTS=1 with Docker available')
class PostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.psycopg=psycopg
        cls.name='transit-export-test-'+uuid.uuid4().hex[:8]
        cls.password=secrets.token_hex(24)
        environment=dict(os.environ,POSTGRES_PASSWORD=cls.password)
        subprocess.run(['docker','run','-d','--rm','--name',cls.name,'-e','POSTGRES_PASSWORD',
                        '-p','127.0.0.1::5432','postgres:16-alpine'],env=environment,check=True,capture_output=True)
        try:
            port=subprocess.check_output(['docker','port',cls.name,'5432/tcp'],text=True).strip().rsplit(':',1)[1]
            cls.options=dict(host='localhost',port=port,user='postgres',password=cls.password,dbname='postgres',options='-c timezone=UTC')
            for _ in range(60):
                try:
                    with cls.connect() as connection:
                        connection.execute('CREATE TABLE transit_events(event_id uuid, vehicle_id text, route_id text, latitude float8, longitude float8, speed float8, event_time timestamptz, received_at timestamptz DEFAULT clock_timestamp())')
                    break
                except psycopg.OperationalError:
                    time.sleep(.5)
            else:
                raise RuntimeError('Test database did not start')
        except Exception:
            subprocess.run(['docker','stop',cls.name],check=True,capture_output=True)
            raise

    @classmethod
    def connect(cls):
        return cls.psycopg.connect(**cls.options)

    @classmethod
    def tearDownClass(cls):
        subprocess.run(['docker','stop',cls.name],check=True,capture_output=True)

    def test_inflight_insert_is_archived_and_snapshot_does_not_hold_writer_lock(self):
        start=parse_hour('2020-01-01T00:00:00Z')
        event=str(uuid.uuid4())
        outcome=[]
        errors=[]
        with tempfile.TemporaryDirectory() as directory, self.connect() as pending:
            pending.execute("INSERT INTO transit_events VALUES (%s,'v','r',41,-87,30,%s,%s)",(event,start,start))
            def export():
                try:
                    # Once the snapshot is established, another writer must remain unblocked.
                    def copy(rows,path):
                        with self.connect() as writer:
                            writer.execute("SET LOCAL lock_timeout = '1s'")
                            writer.execute("INSERT INTO transit_events(event_id,vehicle_id) VALUES (%s,'new')",(uuid.uuid4(),))
                        return write_gzip(rows,path)
                    with patch('export_telemetry.write_gzip',side_effect=copy):
                        outcome.append(read_window(self.connect,start,start+HOUR,Path(directory)/'events.gz'))
                except Exception as error:
                    errors.append(error)
            worker=threading.Thread(target=export)
            worker.start()
            time.sleep(.25)
            self.assertTrue(worker.is_alive(),'Exporter must wait for the in-flight insert')
            pending.commit()
            worker.join(10)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors,[])
            self.assertEqual(outcome,[1])
            rows=gzip.decompress((Path(directory)/'events.gz').read_bytes()).decode().splitlines()
            self.assertEqual(json.loads(rows[0])['event_id'],event)


if __name__=='__main__':
    unittest.main()
