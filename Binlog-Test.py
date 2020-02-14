from __future__ import absolute_import

import json
import logging
import argparse
import apache_beam as beam

def parse_pubsub(line):
    from datetime import datetime

    record = json.loads(line.decode('utf-8'))

    data = {}
    data['processTimestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['triggerTimestamp'] = datetime.fromtimestamp(record['ts']).strftime('%Y-%m-%d %H:%M:%S')
    data['type'] = record['type']
    data['database'] = record['database']
    data['table'] = record['table']
    data['data'] = str(record['data'])
    data['raw'] = str(record)

    return data

def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--subscription', required=True,
      help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    parser.add_argument(
      '--dest_table', required=True,
      help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE')
    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as p:
        lines = ( p | 'Read from PubSub' >> (beam.io.ReadFromPubSub(subscription=known_args.subscription).with_output_types(bytes))
                    | 'Parse message' >> (beam.Map(parse_pubsub))
                    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                        known_args.dest_table,
                        schema = 'processTimestamp:TIMESTAMP, triggerTimestamp:TIMESTAMP, type:STRING, database:STRING, table:STRING, data:STRING,raw:STRING',
                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                    )
            )

if __name__=='__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
