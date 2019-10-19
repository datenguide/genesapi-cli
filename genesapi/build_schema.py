"""
build graphql schema out of cubes from `fetch`
"""


import json
import pandas as pd
import logging
import os
import sys

from genesapi.storage import Storage
from genesapi.util import (
    META_KEYS,
    # parallelize,
    slugify_graphql,
    time_to_json,
    clean_description
)


logger = logging.getLogger(__name__)


def _get_schema(cubes):
    res = []
    for cube in cubes:
        logger.log(logging.INFO, 'Loading `%s` ...' % cube.name)
        cube = cube.current.load()
        roots = [v for k, v in cube.dimensions.items() if k.lower() not in META_KEYS and not v.values]
        excludes = tuple([r.name.lower() for r in roots]) + META_KEYS
        statistic = json.dumps(cube.metadata['statistic'], default=time_to_json)
        for root in roots:
            for dim, dimension in cube.dimensions.items():
                if dim.lower() not in META_KEYS:
                    res.append((root.name, root.data.get('title_de'), None, None, None, None, statistic))
                    if dim.lower() not in excludes:
                        res.append((root.name, None, dim, dimension.data.get('title_de'), None, None, None))
                        for value in dimension.values:
                            res.append((root.name, None, dim, None, value.name, value.data.get('title_de'), None))
    return res


def main(args):
    storage = Storage(args.directory)
    cubes = [c for c in storage]
    # FIXME
    # data = parallelize(_get_schema, cubes)
    data = _get_schema(cubes)
    columns = ['root', 'root_name', 'dimension', 'dimension_name', 'value', 'value_name', 'statistic']
    df = pd.DataFrame(
        data,
        columns=columns
    ).drop_duplicates().sort_values(columns, na_position='first')
    df['root_name'] = df['root_name'].fillna(method='bfill')
    df['statistic'] = df['statistic'].fillna(method='bfill')
    df['dimension_name'] = df['dimension_name'].fillna(method='bfill')
    df['dimension'] = df['dimension'].fillna('').map(slugify_graphql).str.upper()
    df['value'] = df['value'].fillna('').map(slugify_graphql).str.upper()
    df.index = df['root'].map(slugify_graphql).str.upper()
    schema = {}
    for root, data in df.iterrows():
        if root not in schema:
            root_data = {
                'name': data['root_name'],
                'description': None,
                'source': json.loads(data['statistic']),
                'args': {}
            }
            if args.attributes:
                fp = os.path.join(args.attributes, '%s.json' % root)
                if os.path.isfile(fp):
                    with open(fp) as f:
                        desc = json.load(f)
                    root_data.update(description=clean_description(desc['description']))
            schema[root] = root_data
        if data['dimension'] and data['dimension'] not in schema[root]['args']:
            dim_data = {
                'name': data['dimension_name'],
                'description': None,
                'values': []
            }
            if args.attributes:
                fp = os.path.join(args.attributes, '%s.json' % data['dimension'])
                if os.path.isfile(fp):
                    with open(fp) as f:
                        desc = json.load(f)
                    dim_data.update(description=clean_description(desc['description']))
            schema[root]['args'][data['dimension']] = dim_data
        if data['value']:
            schema[root]['args'][data['dimension']]['values'].append({
                'value': data['value'],
                'name': data['value_name']
            })

    sys.stdout.write(json.dumps(schema, default=time_to_json))
    logger.log(logging.INFO, 'Obtained %s roots' % len(df['root'].unique()))
    logger.log(logging.INFO, 'Obtained %s dimensions' % len(df['dimension'].unique()))
    logger.log(logging.INFO, 'Obtained %s values' % len(df['value'].unique()))
