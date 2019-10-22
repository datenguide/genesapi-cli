"""
obtain status of the storage and optionally compare with elastic index
"""


import logging
import pandas as pd
import sys

from elasticsearch import Elasticsearch

from genesapi.storage import Storage
# from genesapi.util import parallelize
from genesapi.util import to_date


logger = logging.getLogger(__name__)


def _get_cubes_data(cubes):
    for cube in cubes:
        yield (
            cube.name,
            cube.last_updated,
            cube.last_exported,
            to_date(cube.metadata['stand'], force_ws=True),
            cube.metadata['status'],
            len(cube.facts)
        )


def main(args):
    logger.info('Obtaining stats for Storage `%s` ...' % args.storage)
    storage = Storage(args.storage)
    # data = parallelize(_get_cubes_data, storage)
    data = _get_cubes_data(storage)
    df = pd.DataFrame(
        (d for d in data),
        columns=('name', 'last_updated', 'last_exported', 'remote_date', 'remote_status', 'facts_count')
    )
    df['storage'] = storage.name
    df = df.sort_values('name')

    logger.info('Total number of facts in Storage `%s`: %s' % (storage, df['facts_count'].sum()))
    ordered_fields = ['storage', 'name', 'last_updated', 'last_exported', 'remote_date', 'remote_status', 'facts_count']
    if args.host and args.index:
        es = Elasticsearch(hosts=[args.host])
        res = es.search(index=args.index, body={'aggs': {'cubes': {'terms': {'field': 'cube', 'size': 20000}}}})  # noqa
        df_es = pd.DataFrame(
            ((c['key'], c['doc_count']) for c in res['aggregations']['cubes']['buckets']),
            columns=('name', 'elastic_facts_count')
        )
        df = df.merge(df_es, on='name', how='outer')
        df['elastic_facts_count'] = df['elastic_facts_count'].fillna(0).map(int)
        logger.info('Total number of facts in Elasticsearch `%s`: %s' % (args.index, df['elastic_facts_count'].sum()))
        ordered_fields += ['elastic_facts_count']

    df['facts_count'] = df['facts_count'].fillna(0).map(int)
    df[ordered_fields].to_csv(sys.stdout, index=False)
    logger.info('Finished obtaining stats for Storage `%s`' % args.storage)
