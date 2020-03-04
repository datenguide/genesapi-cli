"""
build name mapping for regions

- obtain names from storage
- obtain date ranges from ES aggregations

{
    "08425": {
        "id": "08425", // AGS for the region
        "name": "Alb-Donau-Kreis", // Nicely formated name of the region
        "type": "Landkreis", // Type of region (e.g. Kreisfreie Stadt, Regierungsbezirk)
        "level": 3, // NUTS level (1-3), LAU (4)
        "duration": {
            "from": "2012-01-01", // ISO dates for earliest available statistical measure
            "until": "2019-12-31"  // ISO dates for latest available statistical measure
        }
    },
}
"""


import json
import logging
import os
import sys
import pandas as pd

from elasticsearch import Elasticsearch

from genesapi.storage import Storage
from genesapi.util import time_to_json


logger = logging.getLogger(__name__)


def main(args):
    storage = Storage(args.storage)
    regions = {}
    for cube in storage:
        logger.info('Loading `%s` ...' % cube.name)
        for region_id, region in cube.schema.regions.items():
            # take the shortest name
            if region_id in regions:
                if len(regions[region_id]['name']) > len(region['name']):
                    regions[region_id]['name'] = region['name']
            else:
                regions[region_id] = region

    if args.host and args.index:
        logger.info(f'Aggregate dates from ES: {args.host}/{args.index}')
        auth = os.getenv('ELASTIC_AUTH', None)
        es = Elasticsearch(hosts=[args.host], http_auth=auth)
        logger.info(es)
        query = {
            'aggs': {
                'regions': {
                    'terms': {'field': 'region_id', 'size': 16000},
                    'aggs': {
                        'from': {'min': {'field': 'date'}},
                        'until': {'max': {'field': 'date'}}
                    }
                }
            }
        }
        res = es.search(index=args.index, body=query)
        df = pd.DataFrame(res['aggregations']['regions']['buckets'])
        df['from'] = df['from'].map(lambda x: x['value_as_string'])
        df['until'] = df['until'].map(lambda x: x['value_as_string'])
        df.index = df['key']
        for region_id, region in regions.items():
            try:
                enrich = df.loc[region_id]
                region['duration'] = {
                    'from': enrich['from'],
                    'until': enrich['until'],
                }
                region['facts'] = int(enrich['doc_count'])
            except KeyError:
                pass

    sys.stdout.write(json.dumps(regions, default=time_to_json))
