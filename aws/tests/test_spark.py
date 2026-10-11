import sys
import tempfile
import unittest
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from route_analytics import aggregate

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, 'Install aws/requirements-test.txt for local Spark checks')
class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[1]').appName('transit-tests')
                     .config('spark.ui.enabled','false').config('spark.sql.session.timeZone','UTC')
                     .config('spark.sql.shuffle.partitions','1').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_daily_counts_average_duplicates_and_invalid_rows(self):
        at = datetime(2026,10,10,12)
        rows = [('1','v1','r1',10.0,at), ('1','v1','r1',10.0,at), ('2','v2','r1',30.0,at),
                ('3','v3','r2',20.0,at), ('4','v4','r1',40.0,datetime(2026,10,11)),
                ('5','v1','r1',-1.0,at), ('6','v1','r1',float('nan'),at),
                ('7','v1','r1',None,at), ('8','v1',None,20.0,at)]
        events = self.spark.createDataFrame(rows,'event_id string, vehicle_id string, route_id string, speed double, timestamp timestamp')
        result = aggregate(events,'2026-10-10')
        by_route = {r.route_id:r for r in result.collect()}
        self.assertEqual(by_route['r1'].event_count,2)
        self.assertEqual(by_route['r1'].average_speed,20.0)
        self.assertEqual(by_route['r2'].event_count,1)
        with tempfile.TemporaryDirectory() as directory:
            result.write.mode('overwrite').parquet(directory+'/output')
            self.assertEqual(self.spark.read.parquet(directory+'/output').count(),2)
        self.assertEqual(aggregate(events,'2026-10-09').count(),0)

    def test_gzip_jsonl_utc_parsing_and_date_boundary(self):
        import gzip,json
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
        schema=StructType([StructField('event_id',StringType()),StructField('vehicle_id',StringType()),
                           StructField('route_id',StringType()),StructField('speed',DoubleType()),
                           StructField('timestamp',TimestampType())])
        rows=[dict(event_id='utc-1',vehicle_id='v1',route_id='r1',speed=12.0,timestamp='2026-10-11T00:30:00+02:00'),
              dict(event_id='utc-2',vehicle_id='v2',route_id='r1',speed=28.0,timestamp='2026-10-10T23:59:59Z'),
              dict(event_id='utc-3',vehicle_id='v3',route_id='r1',speed=50.0,timestamp='2026-10-11T00:00:00Z')]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'received_date=2026-10-11'/'hour=01'
            path.mkdir(parents=True)
            with gzip.open(path/'events.jsonl.gz','wt') as output:
                for row in rows: output.write(json.dumps(row)+'\n')
            events=(self.spark.read.schema(schema).option('recursiveFileLookup','true')
                    .option('pathGlobFilter','*.jsonl.gz').json(directory))
            result=aggregate(events,'2026-10-10').collect()
            self.assertEqual(result[0].event_count,2)
            self.assertEqual(result[0].average_speed,20.0)


if __name__ == '__main__':
    unittest.main()
