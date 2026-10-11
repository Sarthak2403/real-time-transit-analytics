"""Recompute one UTC event-date partition from archived telemetry."""
import argparse
from datetime import date


def aggregate(events, day):
    from pyspark.sql import functions as F
    valid = events.filter(
        F.col('event_id').isNotNull() & F.col('vehicle_id').isNotNull()
        & F.col('route_id').isNotNull() & F.col('timestamp').isNotNull()
        & F.col('speed').between(0, 160) & ~F.isnan('speed'))
    return (valid.dropDuplicates(['event_id'])
            .withColumn('event_date', F.to_date('timestamp'))
            .filter(F.col('event_date') == F.lit(day).cast('date'))
            .groupBy('event_date', 'route_id')
            .agg(F.count('*').alias('event_count'), F.avg('speed').alias('average_speed')))


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--date', required=True, type=date.fromisoformat)
    args = parser.parse_args()
    spark = SparkSession.builder.appName('transit-daily-routes').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    schema = StructType([StructField('event_id', StringType()), StructField('vehicle_id', StringType()),
                         StructField('route_id', StringType()), StructField('speed', DoubleType()),
                         StructField('timestamp', TimestampType())])
    try:
        events = (spark.read.schema(schema).option('recursiveFileLookup', 'true')
                  .option('pathGlobFilter', '*.jsonl.gz').json(args.input))
        result = aggregate(events, args.date.isoformat())
        result.write.mode('overwrite').parquet(args.output.rstrip('/') + '/event_date=' + args.date.isoformat())
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
