"""End-to-end checks against the running Compose stack; pauses simulator and services."""
import json
import subprocess
import time
import uuid
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = 'http://localhost:8080/api'


def compose(*args, input=None):
    return subprocess.run(['docker', 'compose', *args], cwd=ROOT, input=input,
                          text=True, check=True, capture_output=True).stdout


def api(path, expected=200):
    try:
        with urllib.request.urlopen(BASE + path, timeout=5) as response:
            assert response.status == expected
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 503 or error.code == 500:
            raise urllib.error.URLError('Backend temporarily unavailable') from error
        assert error.code == expected, (path, error.code)


def wait_for(check, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(0.3)
    raise AssertionError('Timed out waiting for processing')


def inside(code, data=''):
    return compose('run', '--rm', '--no-deps', '-T', 'simulator', 'python', '-c', code, input=data)


PUBLISH = '''import sys,json,os
from confluent_kafka import Producer
p=Producer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS'],'enable.idempotence':True,'acks':'all'})
errors=[]
for row in json.load(sys.stdin):
    p.produce(os.getenv('KAFKA_TOPIC','transit-events'),key=row['key'],value=row['value'],on_delivery=lambda e,m: errors.append(str(e)) if e else None)
assert p.flush(30)==0
assert not errors, errors
'''


def publish(events):
    rows = [{'key': e['vehicle_id'], 'value': json.dumps(e)} for e in events]
    inside(PUBLISH, json.dumps(rows))


def main():
    run = uuid.uuid4().hex[:10]
    vehicle = 'test-' + run
    route = 'test-route-' + run
    now = datetime.now(timezone.utc)
    event = dict(event_id=str(uuid.uuid4()), vehicle_id=vehicle, route_id=route,
                 latitude=41.88, longitude=-87.63, speed=30.0, timestamp=now.isoformat())
    stats = lambda: api('/routes/' + route + '/statistics')
    compose('pause', 'simulator')
    try:
        publish([event, event])
        wait_for(lambda: stats()['event_count'] == 1)
        old = dict(event, event_id=str(uuid.uuid4()), speed=10.0,
                   timestamp=(now - timedelta(seconds=30)).isoformat())
        publish([old])
        wait_for(lambda: stats()['event_count'] == 2)
        location = api('/vehicles/' + vehicle + '/location')
        assert location['event_id'] == event['event_id'], location
        assert stats()['average_speed'] == 20.0
        assert stats()['vehicle_count'] == 1
        assert any(v['vehicle_id'] == vehicle for v in api('/vehicles?limit=1000'))
        summary = api('/analytics/summary')
        assert summary['total_events'] >= 2 and summary['total_vehicles'] >= 1
        api('/vehicles/missing-' + run + '/location', 404)
        api('/vehicles?limit=0', 400)
        api('/routes/' + route + '/statistics?from=2020-01-01T00:00:00Z&to=2020-01-03T00:00:00Z', 400)

        invalid = dict(event, event_id=str(uuid.uuid4()), latitude=999)
        publish([invalid])
        dlq_code = '''import json,os,sys,time,uuid
from confluent_kafka import Consumer
wanted=json.load(sys.stdin)['event_id']
c=Consumer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS'],'group.id':'test-'+str(uuid.uuid4()),'auto.offset.reset':'earliest','enable.auto.commit':False})
c.subscribe([os.getenv('KAFKA_DLQ','transit-events-dlq')])
end=time.monotonic()+30
try:
    while time.monotonic()<end:
        m=c.poll(1)
        if m and not m.error() and wanted in m.value().decode():
            assert any(k=='kafka_dlt-exception-message' for k,v in m.headers())
            break
    else: raise AssertionError('Invalid event missing from DLQ')
finally: c.close()
'''
        inside(dlq_code, json.dumps(invalid))
        assert stats()['event_count'] == 2
        compose('stop', 'backend')
        recovered = dict(event, event_id=str(uuid.uuid4()), speed=45.0,
                         timestamp=datetime.now(timezone.utc).isoformat())
        publish([recovered])
        compose('start', 'backend')
        wait_for(lambda: stats()['event_count'] == 3)

        compose('stop', 'postgres')
        database_recovery = dict(event, event_id=str(uuid.uuid4()), timestamp=datetime.now(timezone.utc).isoformat())
        publish([database_recovery])
        time.sleep(3)
        compose('start', 'postgres')
        wait_for(lambda: stats()['event_count'] == 4)

        bench_route = 'bench-' + run
        bench_code = '''import json,sys,time,os
from confluent_kafka import Producer
from simulator import Fleet
route=json.load(sys.stdin)['route']
fleet=Fleet(100,10,seed=42)
p=Producer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP_SERVERS'],'enable.idempotence':True,'acks':'all','linger.ms':10})
errors=[]
start=time.monotonic()
for i in range(2500):
    e=fleet.event(); e['route_id']=route; e['vehicle_id']=route+'-'+e['vehicle_id']
    p.produce(os.getenv('KAFKA_TOPIC','transit-events'),key=e['vehicle_id'],value=json.dumps(e),on_delivery=lambda e,m: errors.append(str(e)) if e else None)
    p.poll(0)
    delay=start+(i+1)/250-time.monotonic()
    if delay>0: time.sleep(delay)
assert p.flush(30)==0
assert not errors, errors
print(json.dumps({'events':2500,'produce_seconds':time.monotonic()-start}))
'''
        started = time.monotonic()
        benchmark = json.loads(inside(bench_code, json.dumps({'route': bench_route})).strip())
        wait_for(lambda: api('/routes/' + bench_route + '/statistics')['event_count'] == 2500)
        elapsed = time.monotonic() - started
        benchmark.update(end_to_end_seconds=elapsed, persisted_events_per_second=2500 / elapsed,
                         target_events_per_second=250, vehicles=100)
        (ROOT / 'benchmark-results.json').write_text(json.dumps(benchmark, indent=2) + '\n')
        print('PASS: duplicates, out-of-order events, REST, validation/DLQ, consumer restart, database recovery')
        print(json.dumps(benchmark, indent=2))
    finally:
        compose('start', 'postgres', 'backend')
        compose('unpause', 'simulator')


if __name__ == '__main__':
    main()
