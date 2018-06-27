"""
build graphql schema out of cubes from `fetch`
"""


import json
import pandas as pd
import logging
import os
import sys

from genesapi.util import (
    get_cube,
    get_files,
    META_KEYS,
    parallelize,
    slugify_graphql,
    time_to_json,
    clean_description
)


logger = logging.getLogger(__name__)


def _get_schema(files):
    res = []
    cubes = (get_cube(fp) for fp in files)
    for cube in cubes:
        logger.log(logging.INFO, 'Loading `%s` ...' % cube.name)
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
    directory = args.directory
    files = []
    if os.path.isdir(directory):
        files = get_files(directory, lambda x: x.endswith('.csv'))
    else:
        logger.log(logging.ERROR, 'data source `%s` not valid.' % directory)

    data = parallelize(_get_schema, files)
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
            schema[root] = {
                'name': data['root_name'],
                'description': None,
                'source': json.loads(data['statistic']),
                'dtype': 'str',
                'args': {}
            }
            if args.keys_directory:
                fp = os.path.join(args.keys_directory, '%s_de.json' % root)
                if os.path.isfile(fp):
                    with open(fp) as f:
                        desc = json.load(f)
                    schema[root].update(description=clean_description(desc['description']))
        if data['dimension'] and data['dimension'] not in schema[root]['args']:
            schema[root]['args'][data['dimension']] = {
                'name': data['dimension_name'],
                'values': []
            }
        if data['value']:
            schema[root]['args'][data['dimension']]['values'].append({
                'value': data['value'],
                'name': data['value_name']
            })

    sys.stdout.write(json.dumps(schema, default=time_to_json))
    logger.log(logging.INFO, 'Obtained %s roots' % len(df['root'].unique()))
    logger.log(logging.INFO, 'Obtained %s dimensions' % len(df['dimension'].unique()))
    logger.log(logging.INFO, 'Obtained %s values' % len(df['value'].unique()))
