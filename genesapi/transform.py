"""
process data pipeline based on cleaned csv tables:

build 1 single key->value database out of many tables
key is a tuple of path items

prints 1 huge table to stdout
"""


import json
import logging
import pandas as pd
import os
import sys

from genesapi.util import get_files, parallelize, CPUS


logger = logging.getLogger(__name__)


def _process_rows(rows, source):
    res = []
    for id_, data in rows:
        date = data.get('_date')
        year = data.get('_year')
        for key, value in data.items():
            if not key.startswith('_'):
                path = [id_] + key.split('__')
                if year:
                    path += [year]
                res.append((id_, source, date, tuple(path), value))
    return res


def main(args):
    dtypes = {}
    if args.dtypes:
        with open(args.dtypes) as f:
            dtypes = json.load(f)

    logger.log(logging.DEBUG, 'Use %s cores on this machine.' % CPUS)

    files = []
    if os.path.isfile(args.source):
        files.append(args.source)
    elif os.path.isdir(args.source):
        files = get_files(args.source, lambda x: x.endswith('.cleaned'))
    else:
        logger.log(logging.ERROR, 'source `%s` not valid.' % args.source)

    chunks = []
    for fp in files:
        logger.log(logging.INFO, 'loading `%s` ...' % fp)
        source = os.path.split(fp)[1].split('.')[0]
        with open(fp) as f:
            columns = f.readline().strip().split(',')
            logger.log(logging.DEBUG, 'columns: %s' % ','.join(columns))
        _dtypes = {k: v for k, v in dtypes.items() if k in columns and '__' not in v}
        df = pd.read_csv(fp, dtype=_dtypes)
        df.index = df['_id']
        chunks.append(parallelize(_process_rows, list(df.iterrows()), source))

    logger.log(logging.INFO, 'wrangling it all together ...')
    df = pd.DataFrame(
        [row for chunk in chunks for row in chunk],
        columns=('id', 'source', 'date', 'path', 'value')
    )
    df = df.drop_duplicates(subset=('path', 'date'))
    df = df.sort_values(['path', 'date'])
    df['path'] = df['path'].map(lambda x: '.'.join(x))

    if args.head:
        df = df.head(args.head)

    df.to_csv(sys.stdout, index=False)

    logger.log(logging.INFO, 'Wrangled %s properties.' % df.shape[0])
