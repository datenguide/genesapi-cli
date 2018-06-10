"""
build a tree-like database interface out of a
single master path->value `pd.DataFrame`
"""

import logging
import json
import os
import pandas as pd
import sys

from collections import defaultdict

from genesapi.util import cast_value, parallelize


logger = logging.getLogger(__name__)


def _tree():
    return defaultdict(_tree)


def _add_path(t, path, value=None):
    for i, p in enumerate(path):
        if value and i+1 == len(path):
            t[p] = value
        else:
            t = t[p]


def _add_to_tree(t, path, value=None):
    # FIXME reorganise code
    if value:
        value = cast_value(value)
    _add_path(t, path, value)
    if ':' in path[-1]:
        leaf = path[-2]
        field, query = path[-1].split(':')
        query_path = path[:-2] + ('%s__%ss' % (leaf, field), '_' + query)
        _add_path(t, query_path, value)


def _get_data_tree(chunk):
    id_, df = chunk
    logger.log(logging.DEBUG, id_)
    df['path'] = df['path'].map(lambda x: tuple(x.split('.')))
    t = _tree()
    for _, data in df.iterrows():
        _add_to_tree(t, data['path'], data['value'])
    return id_, t


def _get_data_trees(chunks):
    return [_get_data_tree(chunk) for chunk in chunks]


def _get_key_tree(df):
    t = _tree()
    df['path'] = df['path'].map(lambda x: tuple(x.split('.')))
    for path in df['path'].map(lambda x: x[1:]).unique():
        _add_to_tree(t, path)
    return t


def _fix_tree(tree):
    return json.dumps(tree).replace('{}', 'null').replace('NaN', 'null')


def _save_data_trees(trees, target):
    for id_, tree in trees:
        tree['id'] = id_
        tree = _fix_tree(tree)
        fp = os.path.join(target, '%s.json' % id_)
        with open(fp, 'w') as f:
            f.write(tree)
        logger.log(logging.DEBUG, 'Wrote %s to `%s` .' % (id_, fp))


def main(args):
    logger.log(logging.INFO, 'Building tree out of `%s` ...' % args.source)
    df = pd.read_csv(args.source, dtype={'id': str})
    ids = df['id'].unique()
    chunks = [(id_, df[df['id'] == id_]) for id_ in ids]
    trees = parallelize(_get_data_trees, chunks)

    logger.log(logging.INFO, 'write key tree to `%s` ...' % args.key_tree)
    key_tree = _get_key_tree(df)
    key_tree['id'] = {}
    with open(args.key_tree, 'w') as f:
        json.dump(key_tree, f)

    if args.split:
        logger.log(logging.INFO, 'Saving trees to `%s` ...' % args.split)
        parallelize(_save_data_trees, trees, args.split)
    else:
        logger.log(logging.INFO, 'Building complete tree ...')
        tree = {id_: t[id_] for id_, t in trees}
        # add ids
        for id_ in ids:
            tree[id_]['id'] = id_
        # import ipdb; ipdb.set_trace()
        tree = _fix_tree(tree)
        sys.stdout.write(tree)

    # fix Hamburg and Berlin FIXME think about that.
    # data_tree['02000'] = json.loads(json.dumps(data_tree['02']))
    # data_tree['02000']['nuts']['level'] = 3
    # data_tree['02000']['id'] = '02000'
    # data_tree['02000']['name_ext'] = 'Hansestadt'
    # data_tree['02000']['slug'] = 'hamburg'
    # data_tree['11000'] = json.loads(json.dumps(data_tree['11']))
    # data_tree['11000']['nuts']['level'] = 3
    # data_tree['11000']['id'] = '11000'
    # data_tree['11000']['name_ext'] = 'Hauptstadt'
    # data_tree['11000']['slug'] = 'berlin'
